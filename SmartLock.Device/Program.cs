using SmartLock.Device;

// 用法: dotnet run --project SmartLock.Device -- <deviceId>
// 未帶參數時預設 deviceId = lock-001，broker 連線資訊參考 DeviceOptions.FromEnvironment。
var options = DeviceOptions.FromEnvironment(args);

Console.WriteLine("=== SmartLock.Device：模擬智慧門鎖裝置 ===");
Console.WriteLine($"DeviceId    : {options.DeviceId}");
Console.WriteLine($"Broker      : {options.BrokerHost}:{options.BrokerPort}");
Console.WriteLine("按 Ctrl+C 結束。");
Console.WriteLine();

using var cancellationTokenSource = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    cancellationTokenSource.Cancel();
};

// Intentionally not disposed on Ctrl+C: the demo relies on the process dropping the TCP connection
// without a DISCONNECT packet so that the broker publishes the Last Will (see README).
var simulator = new DeviceSimulator(options);
await simulator.StartAsync(cancellationTokenSource.Token);

// 裝置本身不會主動結束，靠事件處理指令、狀態、重連；這裡只是讓行程保持存活。
try
{
    await Task.Delay(Timeout.Infinite, cancellationTokenSource.Token);
}
catch (TaskCanceledException)
{
    Console.WriteLine("結束中...");
}
