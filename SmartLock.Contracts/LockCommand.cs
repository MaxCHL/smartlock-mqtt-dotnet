namespace SmartLock.Contracts;

/// <summary>
/// 門鎖動作類型。
/// </summary>
public enum LockAction
{
    Unlock,
    Lock
}

/// <summary>
/// 後端下發給裝置的開關鎖指令。
/// CommandId 是整個系統做 idempotency(冪等性)判斷的關鍵：
/// MQTT QoS 1 或網路重試都可能造成同一筆指令被投遞兩次以上，
/// 裝置端必須用 CommandId 辨識「這筆指令是不是已經處理過」，
/// 避免重複執行造成「重複開鎖」這種安全性問題。
/// </summary>
public sealed class LockCommand
{
    /// <summary>
    /// 指令的唯一識別碼，由後端在建立指令時產生一次，
    /// 後續所有的重送(retry)都必須沿用同一個 CommandId。
    /// </summary>
    public Guid CommandId { get; init; } = Guid.NewGuid();

    public string DeviceId { get; init; } = string.Empty;

    public LockAction Action { get; init; }

    public DateTimeOffset IssuedAtUtc { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// 指令存活時間(秒)。超過這個時間後端就不再等待執行結果，
    /// 直接把這筆指令標記為 Expired，避免狀態機一直卡在 Pending/Accepted。
    /// </summary>
    public int TtlSeconds { get; init; } = 30;

    /// <summary>
    /// 判斷這筆指令相對於指定時間點是否已經過期。
    /// </summary>
    public bool IsExpired(DateTimeOffset nowUtc) => nowUtc > IssuedAtUtc.AddSeconds(TtlSeconds);
}
