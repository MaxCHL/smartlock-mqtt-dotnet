namespace SmartLock.Backend;

/// <summary>
/// 後端連線設定，集中管理方便 demo 時說明設定來源。
/// 範例用環境變數 + 預設值；正式環境建議改用 appsettings.json / K8s ConfigMap + Secret，
/// 並開啟 TLS/mTLS(參考 SmartLock.Device 專案內的 BuildClientOptions 註解)。
/// </summary>
public sealed class BackendOptions
{
    /// <summary>
    /// 本次 demo 要下發指令的目標裝置編號。
    /// </summary>
    public string TargetDeviceId { get; init; } = "lock-001";

    public string BrokerHost { get; init; } = "localhost";

    public int BrokerPort { get; init; } = 1883;

    public static BackendOptions FromEnvironment(string[] args)
    {
        var targetDeviceId = args.Length > 0 && !string.IsNullOrWhiteSpace(args[0])
            ? args[0]
            : Environment.GetEnvironmentVariable("TARGET_DEVICE_ID") ?? "lock-001";

        var brokerHost = Environment.GetEnvironmentVariable("MQTT_BROKER_HOST") ?? "localhost";

        var brokerPort = 1883;
        var portEnv = Environment.GetEnvironmentVariable("MQTT_BROKER_PORT");
        if (!string.IsNullOrWhiteSpace(portEnv) && int.TryParse(portEnv, out var parsedPort))
        {
            brokerPort = parsedPort;
        }

        return new BackendOptions
        {
            TargetDeviceId = targetDeviceId,
            BrokerHost = brokerHost,
            BrokerPort = brokerPort
        };
    }
}
