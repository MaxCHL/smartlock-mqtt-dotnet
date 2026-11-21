namespace SmartLock.Contracts;

/// <summary>
/// 裝置實際執行指令(開鎖/上鎖)之後回報的最終結果。
/// 後端收到後把狀態機從 Accepted 推進到 Executed(成功)或 Failed(失敗)。
/// </summary>
public sealed class CommandResult
{
    public Guid CommandId { get; init; }

    public string DeviceId { get; init; } = string.Empty;

    public bool Success { get; init; }

    /// <summary>
    /// 失敗時的錯誤代碼；成功時應為 null。
    /// </summary>
    public string? ErrorCode { get; init; }

    public DateTimeOffset ExecutedAtUtc { get; init; } = DateTimeOffset.UtcNow;
}
