using System.Collections.Concurrent;
using SmartLock.Contracts;

namespace SmartLock.Backend;

/// <summary>
/// 追蹤所有「後端已下發、尚未確定最終結果」的指令，並驅動每一筆指令的狀態機轉移。
/// 手寫 enum + switch 版本的狀態機：<see cref="TryTransition"/> 用 switch 表達
/// 「目前狀態 -> 目標狀態」是否合法，不合法就拒絕轉移並記錄警告(例如收到重複的 ACK)。
/// </summary>
public sealed class CommandTracker
{
    private readonly ConcurrentDictionary<Guid, TrackedCommand> _commands = new();

    /// <summary>
    /// 後端每下發一筆新指令時呼叫，登記成 Pending 狀態。
    /// </summary>
    public TrackedCommand RegisterPending(LockCommand command)
    {
        var tracked = new TrackedCommand
        {
            CommandId = command.CommandId,
            DeviceId = command.DeviceId,
            Action = command.Action,
            IssuedAtUtc = command.IssuedAtUtc,
            TtlSeconds = command.TtlSeconds,
            State = CommandState.Pending
        };

        _commands[command.CommandId] = tracked;
        Log(tracked, from: null, to: CommandState.Pending, note: "指令已下發");
        return tracked;
    }

    internal TrackedCommand? Get(Guid commandId) =>
        _commands.TryGetValue(commandId, out var tracked) ? tracked : null;

    /// <summary>
    /// 收到裝置的 ACK：Pending -> Accepted。
    /// </summary>
    public void OnAckReceived(CommandAck ack)
    {
        if (!_commands.TryGetValue(ack.CommandId, out var tracked))
        {
            Console.WriteLine($"[Backend] 收到未知指令 {ack.CommandId} 的 ACK，忽略(可能是重啟前的舊指令)。");
            return;
        }

        TryTransition(tracked, CommandState.Accepted, "收到裝置 ACK");
    }

    /// <summary>
    /// 收到裝置的執行結果：Accepted -> Executed 或 Failed。
    /// </summary>
    public void OnResultReceived(CommandResult result)
    {
        if (!_commands.TryGetValue(result.CommandId, out var tracked))
        {
            Console.WriteLine($"[Backend] 收到未知指令 {result.CommandId} 的執行結果，忽略。");
            return;
        }

        var targetState = result.Success ? CommandState.Executed : CommandState.Failed;
        var note = result.Success ? "裝置回報執行成功" : $"裝置回報執行失敗: {result.ErrorCode}";
        TryTransition(tracked, targetState, note);
    }

    /// <summary>
    /// 背景週期性檢查：仍停留在 Pending 或 Accepted、但已經超過 TTL 的指令，一律轉為 Expired。
    /// 這避免了「裝置離線導致永遠不會回 ACK/result」時，指令狀態卡住不動的問題。
    /// </summary>
    public void CheckExpirations(DateTimeOffset nowUtc)
    {
        foreach (var tracked in _commands.Values)
        {
            // Cheap lock-free pre-check; the authoritative check is repeated under the lock below.
            if (tracked.State is CommandState.Executed or CommandState.Failed or CommandState.Expired)
            {
                continue;
            }

            lock (tracked)
            {
                // Re-check under the lock: a result may have landed since the pre-check above.
                if ((tracked.State is CommandState.Pending or CommandState.Accepted) && tracked.IsExpired(nowUtc))
                {
                    ApplyTransition(tracked, CommandState.Expired, "背景檢查發現已超過 TTL 仍未有最終結果");
                }
            }
        }
    }

    /// <summary>
    /// 手寫狀態機的核心：用 switch 表達合法的狀態轉移規則。
    /// 不合法的轉移(例如已經 Executed 的指令又收到一次 ACK)不會被套用，只會記錄警告，
    /// 避免因為訊息重複投遞而讓狀態機被意外撥回或跳躍。
    /// MQTT 回呼與 TTL 計時器會並行呼叫，所以「讀目前狀態 + 檢查 + 寫入」整段以單一指令為單位上鎖，保持原子性。
    /// </summary>
    private void TryTransition(TrackedCommand tracked, CommandState to, string note)
    {
        lock (tracked)
        {
            ApplyTransition(tracked, to, note);
        }
    }

    // Caller must hold the lock on <paramref name="tracked"/>.
    private static void ApplyTransition(TrackedCommand tracked, CommandState to, string note)
    {
        var from = tracked.State;

        // A repeated terminal result (e.g. a re-send hitting the device idempotency cache) is expected, not an error.
        if (from == to && (to is CommandState.Executed or CommandState.Failed))
        {
            Console.WriteLine($"[Backend] 指令 {tracked.CommandId} 收到重複結果 ({to})，依冪等原則忽略。");
            return;
        }

        // A second ACK while already Accepted is a duplicate delivery: rejected with a warning, state unchanged.
        if (from == CommandState.Accepted && to == CommandState.Accepted)
        {
            Console.WriteLine($"[Backend] 指令 {tracked.CommandId} 收到重複的 ACK (已是 Accepted)，拒絕重複轉移。");
            return;
        }

        var isValid = (from, to) switch
        {
            (CommandState.Pending, CommandState.Accepted) => true,
            (CommandState.Pending, CommandState.Expired) => true,
            (CommandState.Accepted, CommandState.Executed) => true,
            (CommandState.Accepted, CommandState.Failed) => true,
            (CommandState.Accepted, CommandState.Expired) => true,
            _ => false
        };

        if (!isValid)
        {
            Console.WriteLine(
                $"[Backend] 指令 {tracked.CommandId} 忽略不合法的狀態轉移: {from} -> {to}" +
                $"(可能是重複訊息，例如 QoS 重送造成的重複 ACK)。");
            return;
        }

        tracked.State = to;
        Log(tracked, from, to, note);
    }

    private static void Log(TrackedCommand tracked, CommandState? from, CommandState to, string note)
    {
        var fromLabel = from?.ToString() ?? "(新建)";
        Console.WriteLine(
            $"[Backend] [{tracked.DeviceId}] 指令 {tracked.CommandId} ({tracked.Action}) " +
            $"狀態轉移: {fromLabel} -> {to}  | {note}");
    }
}
