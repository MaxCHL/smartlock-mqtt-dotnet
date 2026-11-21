using System.Text.Json;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using SmartLock.Contracts;

namespace SmartLock.Backend;

/// <summary>
/// 後端指令派送與狀態追蹤服務：
///   - 對指定裝置下發開關鎖指令，並用 <see cref="CommandTracker"/> 追蹤狀態機。
///   - 訂閱所有裝置的 ACK / 執行結果 / 狀態 topic，驅動狀態機與裝置狀態更新。
///   - 背景週期性檢查逾時未完成的指令，轉為 Expired。
/// </summary>
public sealed class BackendService : IAsyncDisposable
{
    private readonly BackendOptions _options;
    private readonly IMqttClient _mqttClient;
    private readonly CommandTracker _commandTracker = new();
    private readonly DeviceStatusTracker _statusTracker = new();

    // Cancelled on dispose so that a pending reconnect attempt ends instead of outliving the service.
    private readonly CancellationTokenSource _stopping = new();
    private Task _reconnectTask = Task.CompletedTask;

    // Guards _disposed and _reconnectTask so a disconnect racing DisposeAsync either starts a reconnect that
    // DisposeAsync will then observe, or sees _disposed and does nothing. Never held across an await.
    private readonly object _lifecycleGate = new();
    private bool _disposed;

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// 記住最後一次下發的指令，供 demo 用的「resend」功能重送同一個 CommandId，
    /// 藉此在裝置端觸發 idempotency 檢查的分支。
    /// </summary>
    private LockCommand? _lastCommand;

    // Test seams (visible to SmartLock.Tests via InternalsVisibleTo).
    internal CommandTracker CommandTracker => _commandTracker;

    internal DeviceStatusTracker StatusTracker => _statusTracker;

    internal LockCommand? LastCommand => _lastCommand;

    public BackendService(BackendOptions options)
    {
        _options = options;

        var factory = new MqttClientFactory();
        _mqttClient = factory.CreateMqttClient();

        _mqttClient.ConnectedAsync += OnConnectedAsync;
        _mqttClient.DisconnectedAsync += OnDisconnectedAsync;
        _mqttClient.ApplicationMessageReceivedAsync += OnApplicationMessageReceivedAsync;
    }

    // MQTTnet 5 defaults to MQTT 5.0; pin 3.1.1 to keep the protocol this sample was designed and tested against.
    private MqttClientOptions BuildClientOptions() =>
        new MqttClientOptionsBuilder()
            .WithClientId("backend-service")
            .WithTcpServer(_options.BrokerHost, _options.BrokerPort)
            .WithProtocolVersion(MqttProtocolVersion.V311)
            .WithCleanSession()
            .Build();

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var options = BuildClientOptions();

        Console.WriteLine($"[Backend] 連線到 {_options.BrokerHost}:{_options.BrokerPort} ...");
        await _mqttClient.ConnectAsync(options, cancellationToken);
    }

    /// <summary>
    /// 背景檢查迴圈：每秒掃描一次是否有指令超過 TTL 仍未完成。
    /// </summary>
    public async Task RunExpirationWatcherAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            _commandTracker.CheckExpirations(DateTimeOffset.UtcNow);
        }
    }

    /// <summary>
    /// 對目標裝置下發一筆新的開關鎖指令(每次呼叫都是全新的 CommandId)。
    /// </summary>
    public async Task SendNewCommandAsync(LockAction action)
    {
        var command = new LockCommand
        {
            DeviceId = _options.TargetDeviceId,
            Action = action,
            IssuedAtUtc = DateTimeOffset.UtcNow,
            TtlSeconds = 30
        };

        _lastCommand = command;
        _commandTracker.RegisterPending(command);
        await PublishCommandAsync(command);
    }

    /// <summary>
    /// Demo 用：重送最後一筆指令，CommandId 與原本完全相同。
    /// 用來示範「同一筆指令被重複投遞時，裝置端的 idempotency 檢查會擋下重複執行」。
    /// </summary>
    public async Task ResendLastCommandAsync()
    {
        if (_lastCommand is null)
        {
            Console.WriteLine("[Backend] 目前沒有可重送的指令，請先送出一筆指令。");
            return;
        }

        Console.WriteLine($"[Backend] 重送指令 {_lastCommand.CommandId}(CommandId 不變，模擬網路重試)...");
        await PublishCommandAsync(_lastCommand);
    }

    private Task PublishCommandAsync(LockCommand command)
    {
        var message = new MqttApplicationMessageBuilder()
            .WithTopic(Topics.Command(command.DeviceId))
            .WithPayload(JsonSerializer.Serialize(command, JsonOptions))
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .Build();

        return _mqttClient.PublishAsync(message);
    }

    private async Task OnConnectedAsync(MqttClientConnectedEventArgs args)
    {
        Console.WriteLine("[Backend] 已連線，訂閱 ACK / 執行結果 / 狀態 topic。");

        await _mqttClient.SubscribeAsync(
            new MqttTopicFilterBuilder().WithTopic(Topics.CommandAckWildcard()).WithAtLeastOnceQoS().Build());
        await _mqttClient.SubscribeAsync(
            new MqttTopicFilterBuilder().WithTopic(Topics.CommandResultWildcard()).WithAtLeastOnceQoS().Build());
        await _mqttClient.SubscribeAsync(
            new MqttTopicFilterBuilder().WithTopic(Topics.StatusWildcard()).WithAtLeastOnceQoS().Build());
    }

    private Task OnDisconnectedAsync(MqttClientDisconnectedEventArgs args)
    {
        lock (_lifecycleGate)
        {
            if (_disposed)
            {
                return Task.CompletedTask;
            }

            Console.WriteLine($"[Backend] 已斷線，原因: {args.Reason}。5 秒後嘗試重新連線。");
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
            // Service is shutting down.
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Backend] 重新連線失敗: {ex.Message}");
        }
    }

    /// <summary>
    /// 停止重連、乾淨斷線(正常斷線不會觸發 Last Will)並釋放 MQTT client。
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
            Console.WriteLine($"[Backend] 斷線時發生錯誤: {ex.Message}");
        }

        _mqttClient.Dispose();

        // Disposed last: the reconnect task has finished and OnDisconnectedAsync no longer reads _stopping
        // once _disposed is set, so nothing can observe a disposed token source.
        _stopping.Dispose();
    }

    private Task OnApplicationMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs args)
    {
        var topic = args.ApplicationMessage.Topic;
        var payload = args.ApplicationMessage.ConvertPayloadToString();
        if (string.IsNullOrEmpty(payload))
        {
            return Task.CompletedTask;
        }

        try
        {
            if (topic.EndsWith("/command/ack", StringComparison.Ordinal))
            {
                var ack = JsonSerializer.Deserialize<CommandAck>(payload, JsonOptions);
                if (ack is not null)
                {
                    _commandTracker.OnAckReceived(ack);
                }
            }
            else if (topic.EndsWith("/command/result", StringComparison.Ordinal))
            {
                var result = JsonSerializer.Deserialize<CommandResult>(payload, JsonOptions);
                if (result is not null)
                {
                    _commandTracker.OnResultReceived(result);
                }
            }
            else if (topic.EndsWith("/status", StringComparison.Ordinal))
            {
                var status = JsonSerializer.Deserialize<DeviceStatus>(payload, JsonOptions);
                if (status is not null)
                {
                    _statusTracker.OnStatusReceived(status);
                }
            }
        }
        catch (JsonException ex)
        {
            Console.WriteLine($"[Backend] 訊息 JSON 解析失敗 (topic={topic}): {ex.Message}");
        }

        return Task.CompletedTask;
    }
}
