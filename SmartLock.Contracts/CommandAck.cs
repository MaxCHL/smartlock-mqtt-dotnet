namespace SmartLock.Contracts;

/// <summary>
/// 裝置收到指令後「立即」回傳的確認訊息，代表指令已經被接收、
/// 通過 idempotency / TTL 檢查、準備開始執行——但尚未代表真的執行完成。
/// 後端收到 ACK 後把該指令的狀態機從 Pending 推進到 Accepted。
/// </summary>
public sealed class CommandAck
{
    public Guid CommandId { get; init; }

    public string DeviceId { get; init; } = string.Empty;

    public DateTimeOffset AcceptedAtUtc { get; init; } = DateTimeOffset.UtcNow;
}
