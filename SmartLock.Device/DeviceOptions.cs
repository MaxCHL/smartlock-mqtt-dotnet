namespace SmartLock.Device;

/// <summary>
/// 裝置端連線設定，集中於此類別方便展示時說明「設定從哪裡來」。
/// 範例用環境變數 + 預設值，正式環境建議改用 appsettings.json / K8s ConfigMap
/// 搭配 Secret 管理憑證，並開啟下方 BuildClientOptions 註解所述的 TLS/mTLS。
/// </summary>
public sealed class DeviceOptions
{
    public string DeviceId { get; init; } = "lock-001";

    public string BrokerHost { get; init; } = "localhost";

    public int BrokerPort { get; init; } = 1883;

    /// <summary>
    /// 從環境變數與命令列參數組出裝置設定。
    /// 優先序：命令列參數(裝置編號) > 環境變數 > 預設值。
    /// </summary>
    public static DeviceOptions FromEnvironment(string[] args)
    {
        var deviceId = args.Length > 0 && !string.IsNullOrWhiteSpace(args[0])
            ? args[0]
            : Environment.GetEnvironmentVariable("DEVICE_ID") ?? "lock-001";

        var brokerHost = Environment.GetEnvironmentVariable("MQTT_BROKER_HOST") ?? "localhost";

        var brokerPort = 1883;
        var portEnv = Environment.GetEnvironmentVariable("MQTT_BROKER_PORT");
        if (!string.IsNullOrWhiteSpace(portEnv) && int.TryParse(portEnv, out var parsedPort))
        {
            brokerPort = parsedPort;
        }

        return new DeviceOptions
        {
            DeviceId = deviceId,
            BrokerHost = brokerHost,
            BrokerPort = brokerPort
        };
    }
}
