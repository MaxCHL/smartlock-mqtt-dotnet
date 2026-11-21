using SmartLock.Contracts;

namespace SmartLock.Backend;

/// <summary>
/// 後端對單一指令的追蹤記錄：目前狀態 + 判斷 TTL 過期所需要的資訊。
/// </summary>
public sealed class TrackedCommand
{
    public required Guid CommandId { get; init; }

    public required string DeviceId { get; init; }

    public required LockAction Action { get; init; }

    public required DateTimeOffset IssuedAtUtc { get; init; }

    public required int TtlSeconds { get; init; }

    private volatile CommandState _state = CommandState.Pending;

    /// <summary>
    /// Writes must happen under <c>lock(tracked)</c> (see CommandTracker); the volatile field only makes lock-free reads safe.
    /// </summary>
    public CommandState State
    {
        get => _state;
        set => _state = value;
    }

    public bool IsExpired(DateTimeOffset nowUtc) => nowUtc > IssuedAtUtc.AddSeconds(TtlSeconds);
}
