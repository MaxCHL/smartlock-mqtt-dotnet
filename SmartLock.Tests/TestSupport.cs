using System.Net;
using System.Net.Sockets;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Server;
using SmartLock.Contracts;

namespace SmartLock.Tests;

/// <summary>
/// Minimal in-process MQTT broker (MQTTnet.Server) so integration tests need no docker / mosquitto.
/// </summary>
internal sealed class TestBroker : IAsyncDisposable
{
    private MqttServer? _server;

    private const int MaxBindAttempts = 5;

    // Once a start succeeds the port is fixed, so a later restart re-binds the same port (no retry with a new one).
    private bool _portFixed;

    public int Port { get; private set; }

    public TestBroker(int? port = null)
    {
        _portFixed = port is not null;
        Port = port ?? GetFreePort();
    }

    public async Task StartAsync()
    {
        // The free port is probed and released before the server binds it, so another process can grab it in between.
        // Retry with a fresh port a bounded number of times if the bind fails.
        for (var attempt = 1; ; attempt++)
        {
            var options = new MqttServerOptionsBuilder()
                .WithDefaultEndpoint()
                .WithDefaultEndpointPort(Port)
                .Build();

            var server = new MqttServerFactory().CreateMqttServer(options);
            server.ClientConnectedAsync += OnClientConnectedAsync;
            try
            {
                await server.StartAsync();
                _server = server;
                _portFixed = true;
                return;
            }
            catch (Exception) when (!_portFixed && attempt < MaxBindAttempts)
            {
                // Retryable bind failure: release this server, then try again with a fresh port.
                server.Dispose();
                Port = GetFreePort();
            }
            catch
            {
                // Final attempt or fixed-port restart failed: do not leak the server while the exception propagates.
                server.Dispose();
                throw;
            }
        }
    }

    private readonly object _connectionsGate = new();
    private readonly List<(string ClientId, MqttProtocolVersion Version)> _connections = new();

    private Task OnClientConnectedAsync(ClientConnectedEventArgs e)
    {
        lock (_connectionsGate)
        {
            _connections.Add((e.ClientId, e.ProtocolVersion));
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Protocol version of every CONNECT the broker accepted from the given client id, in order.
    /// </summary>
    public IReadOnlyList<MqttProtocolVersion> ConnectionVersions(string clientId)
    {
        lock (_connectionsGate)
        {
            return _connections.Where(c => c.ClientId == clientId).Select(c => c.Version).ToList();
        }
    }

    public async Task StopAsync()
    {
        if (_server is null)
        {
            return;
        }

        await _server.StopAsync();
        _server.Dispose();
        _server = null;
    }

    public ValueTask DisposeAsync() => new(StopAsync());

    /// <summary>
    /// Forcibly drops a client with a non-normal reason so the broker publishes its Last Will.
    /// </summary>
    public Task KillClientAsync(string clientId) =>
        _server!.DisconnectClientAsync(clientId, MQTTnet.Protocol.MqttDisconnectReasonCode.UnspecifiedError);

    public async Task<bool> HasClientAsync(string clientId) =>
        (await _server!.GetClientsAsync()).Any(c => c.Id == clientId);

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}

/// <summary>
/// Test client that subscribes to all command ACK / result topics of one device and counts them.
/// </summary>
internal sealed class CommandObserver : IAsyncDisposable
{
    private readonly IMqttClient _client = new MqttClientFactory().CreateMqttClient();
    private int _acks;
    private int _results;

    public int Acks => Volatile.Read(ref _acks);

    public int Results => Volatile.Read(ref _results);

    public static async Task<CommandObserver> StartAsync(int port, string deviceId)
    {
        var observer = new CommandObserver();
        observer._client.ApplicationMessageReceivedAsync += e =>
        {
            if (e.ApplicationMessage.Topic == Topics.CommandAck(deviceId)) Interlocked.Increment(ref observer._acks);
            if (e.ApplicationMessage.Topic == Topics.CommandResult(deviceId)) Interlocked.Increment(ref observer._results);
            return Task.CompletedTask;
        };
        await observer._client.ConnectAsync(
            new MqttClientOptionsBuilder().WithTcpServer("127.0.0.1", port).WithProtocolVersion(MqttProtocolVersion.V311).Build());
        await observer._client.SubscribeAsync(
            new MqttTopicFilterBuilder().WithTopic($"locks/{deviceId}/command/+").WithAtLeastOnceQoS().Build());
        return observer;
    }

    public Task PublishAsync(MqttApplicationMessage message) => _client.PublishAsync(message);

    public async ValueTask DisposeAsync()
    {
        if (_client.IsConnected)
        {
            await _client.DisconnectAsync();
        }

        _client.Dispose();
    }
}

internal static class Wait
{
    /// <summary>
    /// Polls a condition until it holds or the timeout elapses (no fixed sleeps in tests).
    /// </summary>
    public static async Task UntilAsync(Func<bool> condition, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"Timed out waiting for: {what}");
            }

            await Task.Delay(50);
        }
    }
}
