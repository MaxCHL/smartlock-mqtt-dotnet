using SmartLock.Backend;
using SmartLock.Contracts;

// 用法: dotnet run --project SmartLock.Backend -- <targetDeviceId>
// 未帶參數時預設 targetDeviceId = lock-001，broker 連線資訊參考 BackendOptions.FromEnvironment。
var options = BackendOptions.FromEnvironment(args);

Console.WriteLine("=== SmartLock.Backend：指令派送與狀態追蹤 ===");
Console.WriteLine($"TargetDeviceId : {options.TargetDeviceId}");
Console.WriteLine($"Broker         : {options.BrokerHost}:{options.BrokerPort}");
Console.WriteLine();

using var cancellationTokenSource = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cancellationTokenSource.Cancel();
};

await using var backend = new BackendService(options);
await backend.StartAsync(cancellationTokenSource.Token);

// 背景任務：每秒檢查一次有沒有指令逾時未完成。
var expirationWatcherTask = backend.RunExpirationWatcherAsync(cancellationTokenSource.Token);

// 開機先自動送一筆 Unlock 指令，方便 demo 一啟動就能看到完整的狀態轉移過程。
await backend.SendNewCommandAsync(LockAction.Unlock);

PrintMenu();

while (!cancellationTokenSource.IsCancellationRequested)
{
    // 這裡直接同步讀取主控台輸入即可：MQTT 事件處理與背景 TTL 檢查
    // 都是各自獨立的非同步流程，不會被這裡的 blocking read 卡住。
    var line = Console.ReadLine();
    if (line is null)
    {
        break;
    }

    switch (line.Trim().ToLowerInvariant())
    {
        case "u":
            await backend.SendNewCommandAsync(LockAction.Unlock);
            break;
        case "l":
            await backend.SendNewCommandAsync(LockAction.Lock);
            break;
        case "r":
            // 重送同一個 CommandId，用來示範裝置端的 idempotency 檢查(不會重複開鎖)。
            await backend.ResendLastCommandAsync();
            break;
        case "q":
            cancellationTokenSource.Cancel();
            break;
        default:
            PrintMenu();
            break;
    }
}

try
{
    await expirationWatcherTask;
}
catch (OperationCanceledException)
{
    // 正常結束流程，忽略。
}

Console.WriteLine("結束中...");

static void PrintMenu()
{
    Console.WriteLine();
    Console.WriteLine("指令: [u] 送出 Unlock  [l] 送出 Lock  [r] 重送上一筆指令(demo idempotency)  [q] 離開");
}
