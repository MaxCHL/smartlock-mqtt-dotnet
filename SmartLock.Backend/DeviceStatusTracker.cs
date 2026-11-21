using System.Collections.Concurrent;
using SmartLock.Contracts;

namespace SmartLock.Backend;

/// <summary>
/// 維護後端目前所知、每一台裝置的上下線狀態與最後上線時間(lastSeen)。
/// 資料來源是裝置發佈在 locks/{deviceId}/status 的 retained 訊息，
/// 以及裝置異常斷線時由 broker 代發的 Last Will。
///
/// 範例用記憶體 ConcurrentDictionary 實作；正式環境若後端是多實例部署，
/// 一樣要換成 Redis 這類外部共享儲存，否則每個實例看到的裝置線上狀態會不一致。
/// </summary>
public sealed class DeviceStatusTracker
{
    private readonly ConcurrentDictionary<string, DeviceStatus> _statuses = new();

    public void OnStatusReceived(DeviceStatus status)
    {
        _statuses[status.DeviceId] = status;
        var onlineLabel = status.Online ? "上線" : "離線";
        Console.WriteLine(
            $"[Backend] [{status.DeviceId}] 狀態更新: {onlineLabel}，" +
            $"lastSeen(UTC)={status.LastSeenUtc:O}");
    }

    public DeviceStatus? GetStatus(string deviceId) =>
        _statuses.TryGetValue(deviceId, out var status) ? status : null;
}
