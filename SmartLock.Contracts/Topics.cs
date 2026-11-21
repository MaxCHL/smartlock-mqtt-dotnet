namespace SmartLock.Contracts;

/// <summary>
/// 集中管理 MQTT topic 分層命名規則，避免後端與裝置各自手刻字串造成命名漂移。
///
/// Topic 分層設計：
///   locks/{deviceId}/command          後端 -> 裝置：下發開關鎖指令
///   locks/{deviceId}/command/ack      裝置 -> 後端：指令已收到(尚未執行完成)
///   locks/{deviceId}/command/result   裝置 -> 後端：指令執行結果
///   locks/{deviceId}/status           裝置 -> 後端：上下線狀態(retained + Last Will)
///
/// 用 {deviceId} 分層而不是把所有裝置塞在同一個 topic，
/// 是為了讓 broker 端的 ACL 可以用 topic 前綴做到「裝置只能發佈/訂閱自己的 topic」，
/// 這在正式環境搭配 mTLS 裝置憑證是常見的權限隔離做法。
/// </summary>
public static class Topics
{
    private const string Root = "locks";

    public static string Command(string deviceId) => $"{Root}/{deviceId}/command";

    public static string CommandAck(string deviceId) => $"{Root}/{deviceId}/command/ack";

    public static string CommandResult(string deviceId) => $"{Root}/{deviceId}/command/result";

    public static string Status(string deviceId) => $"{Root}/{deviceId}/status";

    /// <summary>
    /// 後端用萬用字元一次訂閱「所有裝置」的 ACK topic。
    /// </summary>
    public static string CommandAckWildcard() => $"{Root}/+/command/ack";

    /// <summary>
    /// 後端用萬用字元一次訂閱「所有裝置」的執行結果 topic。
    /// </summary>
    public static string CommandResultWildcard() => $"{Root}/+/command/result";

    /// <summary>
    /// 後端用萬用字元一次訂閱「所有裝置」的狀態 topic。
    /// </summary>
    public static string StatusWildcard() => $"{Root}/+/status";

    /// <summary>
    /// 從完整 topic 字串(例如 locks/lock-001/command/ack)解析出 deviceId。
    /// 用於後端訂閱萬用字元 topic 後，需要知道訊息實際來自哪一台裝置。
    /// </summary>
    public static string ExtractDeviceId(string topic)
    {
        var parts = topic.Split('/');
        if (parts.Length < 2 || parts[0] != Root)
        {
            throw new ArgumentException($"不是合法的 locks topic 格式: {topic}", nameof(topic));
        }

        return parts[1];
    }
}
