using System.Collections.Concurrent;
using System.Text.Json;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using SmartLock.Contracts;

namespace SmartLock.Device;

/// <summary>
/// 模擬一台智慧門鎖裝置：連線、維護上下線狀態、接收後端指令並執行。
/// </summary>
public sealed class DeviceSimulator : IAsyncDisposable
{
    private readonly DeviceOptions _options;
    private readonly IMqttClient _mqttClient;

    /// <summary>
    /// Idempotency(冪等性)快取：key 是指令的 CommandId，value 是該指令曾經產生的執行結果。
    /// 範例用 ConcurrentDictionary 存在記憶體裡；正式環境應換成 Redis(或其他外部共享儲存)，
    /// 原因有二：(1) 裝置重啟後記憶體會清空，重啟後收到舊指令的重送會誤判成新指令；
    /// (2) 若裝置服務是多實例部署，記憶體快取無法跨實例共享，仍會發生重複執行。
    /// </summary>
    private readonly ConcurrentDictionary<Guid, CommandResult> _processedCommands = new();

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    // Cancelled on dispose so that a pending reconnect attempt ends instead of outliving the simulator.
    private readonly CancellationTokenSource _stopping = new();
    private Task _reconnectTask = Task.CompletedTask;

    // Guards _disposed and _reconnectTask so a disconnect racing DisposeAsync either starts a reconnect that
    // DisposeAsync will then observe, or sees _disposed and does nothing. Never held across an await.
    private readonly object _lifecycleGate = new();
    private bool _disposed;

    public DeviceSimulator(DeviceOptions options)
    {
        _options = options;

        var factory = new MqttClientFactory();
        _mqttClient = factory.CreateMqttClient();

        _mqttClient.ConnectedAsync += OnConnectedAsync;
        _mqttClient.DisconnectedAsync += OnDisconnectedAsync;
        _mqttClient.ApplicationMessageReceivedAsync += OnApplicationMessageReceivedAsync;
    }

    /// <summary>
    /// 啟動裝置：建立連線設定(含 Last Will)並連線到 broker。
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var options = BuildClientOptions();
        Console.WriteLine($"[Device:{_options.DeviceId}] 連線到 {_options.BrokerHost}:{_options.BrokerPort} ...");
        await _mqttClient.ConnectAsync(options, cancellationToken);
    }

    /// <summary>
    /// 建立 MQTT 連線設定。
    ///
    /// 正式環境安全性注意事項(範例為求簡單使用明文 1883，正式環境務必調整)：
    ///   1. 開啟 TLS，並改用 8883 或加密後的 WebSocket。
    ///   2. 採用 mTLS：每台裝置持有獨立的裝置憑證(device certificate)，
    ///      broker 端驗證憑證身分，取代目前範例完全沒有驗證的匿名連線。
    ///   3. 在 broker 端設定 ACL，限制每個裝置憑證只能發佈/訂閱自己的 locks/{deviceId}/* topic，
    ///      避免裝置冒用其他裝置的身分發送假訊息。
    ///   4. 憑證要有輪替(rotation)機制與到期監控，避免憑證過期後裝置整批離線。
    /// </summary>
    private MqttClientOptions BuildClientOptions()
    {
        // Last Will(遺囑)：一旦裝置與 broker 的連線異常斷開(斷電、斷網、當機等，
        // 而不是裝置主動、乾淨地斷線)，broker 會自動代替裝置發佈這則 Offline 狀態，
        // 且以 retained 訊息保留，讓任何之後才訂閱的後端都能立刻讀到裝置目前離線。
        var willPayload = JsonSerializer.Serialize(
            new DeviceStatus
            {
                DeviceId = _options.DeviceId,
                Online = false,
                LastSeenUtc = DateTimeOffset.UtcNow
            },
            JsonOptions);

        return new MqttClientOptionsBuilder()
            .WithClientId($"device-{_options.DeviceId}")
            .WithTcpServer(_options.BrokerHost, _options.BrokerPort)
            // MQTTnet 5 defaults to MQTT 5.0; pin 3.1.1 to keep the protocol this sample was designed and tested against.
            .WithProtocolVersion(MqttProtocolVersion.V311)
            .WithCleanSession()
            .WithWillTopic(Topics.Status(_options.DeviceId))
            .WithWillPayload(willPayload)
            .WithWillQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .WithWillRetain(true)
            .Build();
    }

    private async Task OnConnectedAsync(MqttClientConnectedEventArgs args)
    {
        Console.WriteLine($"[Device:{_options.DeviceId}] 已連線。訂閱指令 topic 並發佈上線狀態。");

        await _mqttClient.SubscribeAsync(
            new MqttTopicFilterBuilder()
                .WithTopic(Topics.Command(_options.DeviceId))
                .WithAtLeastOnceQoS()
                .Build());

        // 連線成功後立即發佈 retained 的 Online 狀態，
        // 這樣任何之後才連上來的後端訂閱者都能馬上讀到目前是在線的。
        await PublishStatusAsync(online: true);
    }

    private Task OnDisconnectedAsync(MqttClientDisconnectedEventArgs args)
    {
        lock (_lifecycleGate)
        {
            if (_disposed)
            {
                return Task.CompletedTask;
            }

            Console.WriteLine($"[Device:{_options.DeviceId}] 已斷線，原因: {args.Reason}。5 秒後嘗試重新連線。");
            // 範例層級的重連策略：固定延遲後重試。
            // 正式環境應補上指數退避與離線訊息佇列(MQTTnet 5 已沒有 ManagedMqttClient，
            // 需自行封裝或採用社群套件)，而不是像這裡只做固定延遲重試。
            _reconnectTask = ReconnectAsync(_stopping.Token);
        }

        return Task.CompletedTask;
    }

    private async Task ReconnectAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);

            // Cancellation may land right after the delay completes; never touch the client once it is requested.
            cancellationToken.ThrowIfCancellationRequested();
            await _mqttClient.ConnectAsync(BuildClientOptions(), cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Simulator is shutting down.
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Device:{_options.DeviceId}] 重新連線失敗: {ex.Message}");
        }
    }

    /// <summary>
    /// 停止重連、乾淨斷線(正常斷線不會觸發 Last Will，broker 上保留的是最後一次發佈的 Online 狀態)並釋放 MQTT client。
    /// 若要模擬異常斷線，請由 broker 端踢掉連線，而不是呼叫這個方法。
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        Task reconnectTask;
        lock (_lifecycleGate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _stopping.Cancel();
            reconnectTask = _reconnectTask;
        }

        // After the lock no new reconnect can start, so this is the last one to wait for.
        try
        {
            await reconnectTask;
        }
        catch (OperationCanceledException)
        {
            // Expected: the reconnect was cancelled by _stopping.
        }

        try
        {
            if (_mqttClient.IsConnected)
            {
                await _mqttClient.DisconnectAsync(MqttClientDisconnectOptionsReason.NormalDisconnection);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Device:{_options.DeviceId}] 斷線時發生錯誤: {ex.Message}");
        }

        _mqttClient.Dispose();

        // Disposed last: the reconnect task has finished and OnDisconnectedAsync no longer reads _stopping
        // once _disposed is set, so nothing can observe a disposed token source.
        _stopping.Dispose();
    }

    private async Task OnApplicationMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs args)
    {
        if (args.ApplicationMessage.Topic != Topics.Command(_options.DeviceId))
        {
            return;
        }

        var payload = args.ApplicationMessage.ConvertPayloadToString();
        if (string.IsNullOrEmpty(payload))
        {
            Console.WriteLine($"[Device:{_options.DeviceId}] 收到空的指令 payload，忽略。");
            return;
        }

        LockCommand? command;
        try
        {
            command = JsonSerializer.Deserialize<LockCommand>(payload, JsonOptions);
        }
        catch (JsonException ex)
        {
            Console.WriteLine($"[Device:{_options.DeviceId}] 指令 JSON 解析失敗: {ex.Message}");
            return;
        }

        if (command is null)
        {
            return;
        }

        await HandleCommandAsync(command);
    }

    private async Task HandleCommandAsync(LockCommand command)
    {
        // === 關鍵設計點：Idempotency 檢查 ===
        // 這是整個系統最重要的安全防線。MQTT QoS 1「至少一次」保證訊息不會漏送，
        // 但代價是同一則訊息可能被重複投遞；後端也可能因為逾時而重送同一筆指令。
        // 如果每次收到指令都無條件執行，重複投遞/重送就會造成「重複開鎖」——
        // 對門鎖來說這是實際的安全問題(例如門本來要保持鎖住，卻被重送指令又開了一次)。
        // 因此裝置端必須先用 CommandId 查快取：處理過就直接回傳舊結果，絕不重新執行。
        if (_processedCommands.TryGetValue(command.CommandId, out var cachedResult))
        {
            Console.WriteLine(
                $"[Device:{_options.DeviceId}] 指令 {command.CommandId} 先前已處理過(idempotent)，" +
                "直接回傳快取結果，不重複開鎖。");
            await PublishResultAsync(cachedResult);
            return;
        }

        var now = DateTimeOffset.UtcNow;
        if (command.IsExpired(now))
        {
            Console.WriteLine($"[Device:{_options.DeviceId}] 指令 {command.CommandId} 已超過 TTL，拒絕執行。");
            var expiredResult = new CommandResult
            {
                CommandId = command.CommandId,
                DeviceId = _options.DeviceId,
                Success = false,
                ErrorCode = "TTL_EXPIRED",
                ExecutedAtUtc = now
            };
            _processedCommands[command.CommandId] = expiredResult;
            await PublishResultAsync(expiredResult);
            return;
        }

        // 先回 ACK，代表「已收到、通過驗證、準備執行」，但還沒有真正執行完成。
        await PublishAckAsync(command);

        Console.WriteLine($"[Device:{_options.DeviceId}] 執行指令 {command.CommandId}: {command.Action} ...");

        // 模擬馬達動作、感測器回報等實際執行門鎖硬體所需要的時間。
        await Task.Delay(TimeSpan.FromMilliseconds(500));

        var result = new CommandResult
        {
            CommandId = command.CommandId,
            DeviceId = _options.DeviceId,
            Success = true,
            ErrorCode = null,
            ExecutedAtUtc = DateTimeOffset.UtcNow
        };

        // 執行完成後才寫入 idempotency 快取，之後同一個 CommandId 再進來就會直接命中上面的分支。
        _processedCommands[command.CommandId] = result;

        Console.WriteLine($"[Device:{_options.DeviceId}] 指令 {command.CommandId} 執行完成: {command.Action} 成功。");
        await PublishResultAsync(result);
    }

    private Task PublishAckAsync(LockCommand command)
    {
        var ack = new CommandAck
        {
            CommandId = command.CommandId,
            DeviceId = _options.DeviceId,
            AcceptedAtUtc = DateTimeOffset.UtcNow
        };

        var message = new MqttApplicationMessageBuilder()
            .WithTopic(Topics.CommandAck(_options.DeviceId))
            .WithPayload(JsonSerializer.Serialize(ack, JsonOptions))
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .Build();

        return _mqttClient.PublishAsync(message);
    }

    private Task PublishResultAsync(CommandResult result)
    {
        var message = new MqttApplicationMessageBuilder()
            .WithTopic(Topics.CommandResult(_options.DeviceId))
            .WithPayload(JsonSerializer.Serialize(result, JsonOptions))
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .Build();

        return _mqttClient.PublishAsync(message);
    }

    private Task PublishStatusAsync(bool online)
    {
        var status = new DeviceStatus
        {
            DeviceId = _options.DeviceId,
            Online = online,
            LastSeenUtc = DateTimeOffset.UtcNow
        };

        // retained = true：新訂閱者連上來時，broker 會立刻補送這則訊息，
        // 不需要等裝置剛好在那個時間點又發一次狀態。
        var message = new MqttApplicationMessageBuilder()
            .WithTopic(Topics.Status(_options.DeviceId))
            .WithPayload(JsonSerializer.Serialize(status, JsonOptions))
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .WithRetainFlag(true)
            .Build();

        return _mqttClient.PublishAsync(message);
    }
}
