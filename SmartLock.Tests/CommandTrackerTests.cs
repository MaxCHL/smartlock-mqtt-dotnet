using SmartLock.Backend;
using SmartLock.Contracts;

namespace SmartLock.Tests;

[Collection("console")]
public class CommandTrackerTests
{
    private static LockCommand NewCommand(DateTimeOffset? issuedAt = null, int ttl = 30) => new()
    {
        DeviceId = "lock-001",
        Action = LockAction.Unlock,
        IssuedAtUtc = issuedAt ?? DateTimeOffset.UtcNow,
        TtlSeconds = ttl
    };

    private static CommandAck AckFor(LockCommand c) => new() { CommandId = c.CommandId, DeviceId = c.DeviceId };

    private static CommandResult ResultFor(LockCommand c, bool success = true) =>
        new() { CommandId = c.CommandId, DeviceId = c.DeviceId, Success = success, ErrorCode = success ? null : "JAMMED" };

    [Fact]
    public void Valid_lifecycle_goes_Pending_Accepted_Executed()
    {
        var tracker = new CommandTracker();
        var cmd = NewCommand();

        var tracked = tracker.RegisterPending(cmd);
        Assert.Equal(CommandState.Pending, tracked.State);

        tracker.OnAckReceived(AckFor(cmd));
        Assert.Equal(CommandState.Accepted, tracked.State);

        tracker.OnResultReceived(ResultFor(cmd));
        Assert.Equal(CommandState.Executed, tracked.State);
    }

    [Fact]
    public void Failed_result_moves_Accepted_to_Failed()
    {
        var tracker = new CommandTracker();
        var cmd = NewCommand();
        var tracked = tracker.RegisterPending(cmd);

        tracker.OnAckReceived(AckFor(cmd));
        tracker.OnResultReceived(ResultFor(cmd, success: false));

        Assert.Equal(CommandState.Failed, tracked.State);
    }

    [Fact]
    public void Invalid_transition_is_rejected_and_keeps_state()
    {
        var tracker = new CommandTracker();
        var cmd = NewCommand();
        var tracked = tracker.RegisterPending(cmd);

        // A result arriving while still Pending would skip Accepted: must be refused.
        var output = CaptureConsole(() => tracker.OnResultReceived(ResultFor(cmd)));

        Assert.Equal(CommandState.Pending, tracked.State);
        Assert.Contains("忽略不合法的狀態轉移: Pending -> Executed", output);
    }

    [Fact]
    public void Duplicate_Executed_is_idempotent_and_logged_as_non_error()
    {
        var tracker = new CommandTracker();
        var cmd = NewCommand();
        var tracked = tracker.RegisterPending(cmd);
        tracker.OnAckReceived(AckFor(cmd));
        tracker.OnResultReceived(ResultFor(cmd));

        var output = CaptureConsole(() => tracker.OnResultReceived(ResultFor(cmd)));

        Assert.Equal(CommandState.Executed, tracked.State);
        Assert.Contains($"指令 {cmd.CommandId} 收到重複結果 (Executed)，依冪等原則忽略", output);
        Assert.DoesNotContain("不合法", output);
    }

    [Fact]
    public void Duplicate_Failed_is_idempotent_and_logged_as_non_error()
    {
        var tracker = new CommandTracker();
        var cmd = NewCommand();
        var tracked = tracker.RegisterPending(cmd);
        tracker.OnAckReceived(AckFor(cmd));
        tracker.OnResultReceived(ResultFor(cmd, success: false));

        var output = CaptureConsole(() => tracker.OnResultReceived(ResultFor(cmd, success: false)));

        Assert.Equal(CommandState.Failed, tracked.State);
        Assert.Contains($"指令 {cmd.CommandId} 收到重複結果 (Failed)，依冪等原則忽略", output);
    }

    [Fact]
    public void Duplicate_ACK_while_Accepted_is_rejected_and_not_logged_as_idempotent_result()
    {
        var tracker = new CommandTracker();
        var cmd = NewCommand();
        var tracked = tracker.RegisterPending(cmd);
        tracker.OnAckReceived(AckFor(cmd));

        var output = CaptureConsole(() => tracker.OnAckReceived(AckFor(cmd)));

        Assert.Equal(CommandState.Accepted, tracked.State);
        Assert.Contains($"指令 {cmd.CommandId} 收到重複的 ACK (已是 Accepted)", output);
        Assert.DoesNotContain("重複結果", output);
        Assert.DoesNotContain("冪等", output);
    }

    [Fact]
    public void Late_ACK_on_Expired_command_is_rejected_and_not_logged_as_idempotent()
    {
        var tracker = new CommandTracker();
        var issued = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var cmd = NewCommand(issued, ttl: 30);
        var tracked = tracker.RegisterPending(cmd);
        tracker.CheckExpirations(issued.AddSeconds(31));
        Assert.Equal(CommandState.Expired, tracked.State);

        // A late ACK on an Expired command is an invalid transition, not an idempotent duplicate.
        var output = CaptureConsole(() => tracker.OnAckReceived(AckFor(cmd)));

        Assert.Equal(CommandState.Expired, tracked.State);
        Assert.Contains("忽略不合法的狀態轉移: Expired -> Accepted", output);
        Assert.DoesNotContain("冪等", output);
    }

    [Fact]
    public void Conflicting_terminal_result_after_Executed_is_rejected_and_not_logged_as_idempotent()
    {
        var tracker = new CommandTracker();
        var cmd = NewCommand();
        var tracked = tracker.RegisterPending(cmd);
        tracker.OnAckReceived(AckFor(cmd));
        tracker.OnResultReceived(ResultFor(cmd));

        // Executed -> Failed is a different state, so it is not a duplicate.
        var output = CaptureConsole(() => tracker.OnResultReceived(ResultFor(cmd, success: false)));

        Assert.Equal(CommandState.Executed, tracked.State);
        Assert.Contains("忽略不合法的狀態轉移: Executed -> Failed", output);
        Assert.DoesNotContain("冪等", output);
    }

    [Fact]
    public void Concurrent_result_and_expiry_end_in_exactly_one_terminal_transition()
    {
        var tracker = new CommandTracker();
        var issued = DateTimeOffset.UtcNow.AddSeconds(-60); // already past its TTL, so expiry is always eligible
        var commands = Enumerable.Range(0, 300).Select(_ => NewCommand(issued, ttl: 30)).ToList();
        foreach (var cmd in commands)
        {
            tracker.RegisterPending(cmd);
            tracker.OnAckReceived(AckFor(cmd));
        }

        var output = CaptureConsole(() =>
            Parallel.ForEach(commands, new ParallelOptions { MaxDegreeOfParallelism = 8 }, cmd =>
            {
                var result = Task.Run(() => tracker.OnResultReceived(ResultFor(cmd)));
                var expiry = Task.Run(() => tracker.CheckExpirations(DateTimeOffset.UtcNow));
                Task.WaitAll(result, expiry);
            }));

        var lines = output.Split('\n');
        foreach (var cmd in commands)
        {
            var state = tracker.Get(cmd.CommandId)!.State;
            Assert.True(state is CommandState.Executed or CommandState.Expired, $"unexpected state {state}");

            // Exactly one transition out of Accepted may have been applied and logged.
            var applied = lines.Count(l => l.Contains(cmd.CommandId.ToString()) && l.Contains("狀態轉移: Accepted ->"));
            Assert.Equal(1, applied);
        }
    }

    [Fact]
    public void Command_past_ttl_becomes_Expired_but_not_before()
    {
        var tracker = new CommandTracker();
        var issued = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var cmd = NewCommand(issued, ttl: 30);
        var tracked = tracker.RegisterPending(cmd);

        tracker.CheckExpirations(issued.AddSeconds(30));
        Assert.Equal(CommandState.Pending, tracked.State);

        tracker.CheckExpirations(issued.AddSeconds(31));
        Assert.Equal(CommandState.Expired, tracked.State);
    }

    private static string CaptureConsole(Action action)
    {
        var original = Console.Out;
        var writer = new StringWriter();
        Console.SetOut(writer);
        try
        {
            action();
        }
        finally
        {
            Console.SetOut(original);
        }

        return writer.ToString();
    }
}
