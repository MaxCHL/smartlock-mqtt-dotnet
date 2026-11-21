using System.Text.Json;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using SmartLock.Backend;
using SmartLock.Contracts;
using SmartLock.Device;

namespace SmartLock.Tests;

/// <summary>
/// End-to-end tests against an in-process broker. Each test uses its own random port.
/// </summary>
public class BrokerIntegrationTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(12);
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private static async Task<BackendService> StartBackendAsync(int port, string deviceId)
    {
        var backend = new BackendService(new BackendOptions { TargetDeviceId = deviceId, BrokerPort = port, BrokerHost = "127.0.0.1" });
        await backend.StartAsync(CancellationToken.None);
        return backend;
    }

    private static async Task<DeviceSimulator> StartDeviceAsync(int port, string deviceId)
    {
        var device = new DeviceSimulator(new DeviceOptions { DeviceId = deviceId, BrokerPort = port, BrokerHost = "127.0.0.1" });
        await device.StartAsync(CancellationToken.None);
        return device;
    }

    [Fact]
    public async Task Command_round_trip_reaches_Executed_and_resend_is_idempotent()
    {
        await using var broker = new TestBroker();
        await broker.StartAsync();
        await using var observer = await CommandObserver.StartAsync(broker.Port, "lock-e2e");
        await using var backend = await StartBackendAsync(broker.Port, "lock-e2e");
        await using var device = await StartDeviceAsync(broker.Port, "lock-e2e");

        await backend.SendNewCommandAsync(LockAction.Unlock);
        var id = backend.LastCommand!.CommandId;
        await Wait.UntilAsync(() => backend.CommandTracker.Get(id)?.State == CommandState.Executed, Timeout, "Executed");
        await Wait.UntilAsync(() => observer.Results == 1, Timeout, "first result");

        // Re-send with the same CommandId: the device must replay the cached result without executing again.
        await backend.ResendLastCommandAsync();
        await Wait.UntilAsync(() => observer.Results == 2, Timeout, "replayed result");

        Assert.Equal(1, observer.Acks); // an ACK is only sent when the device really executes
        Assert.Equal(CommandState.Executed, backend.CommandTracker.Get(id)!.State);
    }

    [Fact]
    public async Task Same_CommandId_twice_executes_only_once()
    {
        await using var broker = new TestBroker();
        await broker.StartAsync();
        await using var device = await StartDeviceAsync(broker.Port, "lock-idem");

        // Observer counts ACKs (sent only when the device really executes) and results (sent on every delivery).
        await using var observer = await CommandObserver.StartAsync(broker.Port, "lock-idem");

        var payload = JsonSerializer.Serialize(
            new LockCommand { DeviceId = "lock-idem", Action = LockAction.Unlock, TtlSeconds = 30 }, Json);
        MqttApplicationMessage Msg() => new MqttApplicationMessageBuilder()
            .WithTopic(Topics.Command("lock-idem")).WithPayload(payload)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce).Build();

        await observer.PublishAsync(Msg());
        await Wait.UntilAsync(() => observer.Results == 1, Timeout, "first result");
        await observer.PublishAsync(Msg());
        await Wait.UntilAsync(() => observer.Results == 2, Timeout, "replayed result");

        Assert.Equal(1, observer.Acks);
    }

    [Fact]
    public async Task Last_Will_marks_device_offline_when_connection_drops_abruptly()
    {
        await using var broker = new TestBroker();
        await broker.StartAsync();
        await using var backend = await StartBackendAsync(broker.Port, "lock-will");
        await using var device = await StartDeviceAsync(broker.Port, "lock-will");
        await Wait.UntilAsync(() => backend.StatusTracker.GetStatus("lock-will")?.Online == true, Timeout, "online");

        await broker.KillClientAsync("device-lock-will");

        await Wait.UntilAsync(() => backend.StatusTracker.GetStatus("lock-will")?.Online == false, Timeout, "offline via Last Will");
    }

    [Fact]
    public async Task Clean_dispose_disconnects_without_triggering_Last_Will()
    {
        await using var broker = new TestBroker();
        await broker.StartAsync();
        await using var backend = await StartBackendAsync(broker.Port, "lock-bye");
        var device = await StartDeviceAsync(broker.Port, "lock-bye");
        await Wait.UntilAsync(() => backend.StatusTracker.GetStatus("lock-bye")?.Online == true, Timeout, "online");

        await device.DisposeAsync();

        // The broker drops the client, but a graceful DISCONNECT must not publish the retained offline will.
        var deadline = DateTime.UtcNow + Timeout;
        while (await broker.HasClientAsync("device-lock-bye"))
        {
            Assert.True(DateTime.UtcNow < deadline, "device client still connected after DisposeAsync");
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }

        await Task.Delay(300, TestContext.Current.CancellationToken); // settle time for a (wrongly) published will
        Assert.True(backend.StatusTracker.GetStatus("lock-bye")!.Online);
    }

    [Fact]
    public async Task Device_reconnects_and_reports_online_after_broker_restart()
    {
        await using var broker = new TestBroker();
        await broker.StartAsync();
        await using var backend = await StartBackendAsync(broker.Port, "lock-rc");
        await using var device = await StartDeviceAsync(broker.Port, "lock-rc");
        await Wait.UntilAsync(() => backend.StatusTracker.GetStatus("lock-rc")?.Online == true, Timeout, "initial online");
        var firstSeen = backend.StatusTracker.GetStatus("lock-rc")!.LastSeenUtc;

        await broker.StopAsync();
        await broker.StartAsync(); // same port

        // Fixed 5s reconnect delay in both clients, so this takes ~5-6s.
        await Wait.UntilAsync(
            () => backend.StatusTracker.GetStatus("lock-rc") is { Online: true } s && s.LastSeenUtc > firstSeen,
            Timeout, "online again after restart");
    }

    [Fact]
    public async Task Command_to_absent_device_stays_Pending_then_Expires_by_ttl()
    {
        await using var broker = new TestBroker();
        await broker.StartAsync();
        await using var backend = await StartBackendAsync(broker.Port, "lock-absent");

        await backend.SendNewCommandAsync(LockAction.Lock);
        var tracked = backend.CommandTracker.Get(backend.LastCommand!.CommandId)!;
        Assert.Equal(CommandState.Pending, tracked.State);

        // Time is injected, so simulate the TTL elapsing instead of waiting 30s.
        backend.CommandTracker.CheckExpirations(tracked.IssuedAtUtc.AddSeconds(tracked.TtlSeconds + 1));

        Assert.Equal(CommandState.Expired, tracked.State);
    }

    [Fact]
    public async Task Device_and_backend_connect_with_MQTT_3_1_1()
    {
        await using var broker = new TestBroker();
        await broker.StartAsync();
        await using var backend = await StartBackendAsync(broker.Port, "lock-ver");
        await using var device = await StartDeviceAsync(broker.Port, "lock-ver");

        await Wait.UntilAsync(
            () => broker.ConnectionVersions("backend-service").Count == 1 && broker.ConnectionVersions("device-lock-ver").Count == 1,
            Timeout, "both clients connected");

        // The pin guards against MQTTnet 5 silently defaulting to MQTT 5.0.
        Assert.Equal(new[] { MqttProtocolVersion.V311 }, broker.ConnectionVersions("backend-service"));
        Assert.Equal(new[] { MqttProtocolVersion.V311 }, broker.ConnectionVersions("device-lock-ver"));
    }

    [Fact]
    public async Task Disconnect_racing_DisposeAsync_never_throws_and_never_reconnects_after_dispose()
    {
        const int Iterations = 20;
        await using var broker = new TestBroker();
        await broker.StartAsync();
        var rng = new Random(12345);

        // Devices have distinct client ids, so they run in parallel; each races a broker-side kick against dispose.
        var deviceRaces = Enumerable.Range(0, Iterations).Select(async i =>
        {
            var id = $"race-{i}";
            var device = await StartDeviceAsync(broker.Port, id);
            await Wait.UntilAsync(() => broker.ConnectionVersions($"device-{id}").Count == 1, Timeout, "device connected");
            await RaceKickAgainstDisposeAsync(broker, $"device-{id}", device.DisposeAsync, Random.Shared.Next(0, 5));
        }).ToList();

        // The backend uses a fixed client id, so its iterations run one after another.
        var backendRaces = Task.Run(async () =>
        {
            for (var i = 0; i < Iterations; i++)
            {
                var backend = await StartBackendAsync(broker.Port, "race-backend");
                var expected = i + 1;
                await Wait.UntilAsync(() => broker.ConnectionVersions("backend-service").Count == expected, Timeout, "backend connected");
                await RaceKickAgainstDisposeAsync(broker, "backend-service", backend.DisposeAsync, rng.Next(0, 5));
            }
        }, TestContext.Current.CancellationToken);

        await Task.WhenAll(deviceRaces.Append(backendRaces));

        // A reconnect started by a racing disconnect would fire after the fixed 5s delay; wait it out (bounded).
        await Task.Delay(TimeSpan.FromSeconds(6.5), TestContext.Current.CancellationToken);

        for (var i = 0; i < Iterations; i++)
        {
            Assert.Single(broker.ConnectionVersions($"device-race-{i}"));
        }

        Assert.Equal(Iterations, broker.ConnectionVersions("backend-service").Count);
    }

    private static async Task RaceKickAgainstDisposeAsync(TestBroker broker, string clientId, Func<ValueTask> dispose, int jitterMs)
    {
        var kick = Task.Run(async () =>
        {
            try
            {
                await broker.KillClientAsync(clientId);
            }
            catch (Exception)
            {
                // The client may already be gone if dispose won the race; that is the point of the race.
            }
        });
        var disposing = Task.Run(async () =>
        {
            if (jitterMs > 0)
            {
                await Task.Delay(jitterMs, TestContext.Current.CancellationToken);
            }

            await dispose();
        });

        // Neither the disconnect handler nor DisposeAsync may let an exception escape.
        await Task.WhenAll(kick, disposing);
    }
}
