namespace SmartLock.Contracts;

/// <summary>
/// 裝置的上下線狀態。透過「連線時發佈 retained 的 Online 狀態」
/// 搭配「Last Will(遺囑)在異常斷線時由 broker 代發 Offline 狀態」這兩個機制，
/// 讓後端不需要主動 ping 裝置就能得知裝置目前是否在線、以及最後一次上線時間。
/// </summary>
public sealed class DeviceStatus
{
    public string DeviceId { get; init; } = string.Empty;

    public bool Online { get; init; }

    public DateTimeOffset LastSeenUtc { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// 電量百分比(0-100)，選填。並非所有門鎖裝置都有電池回報能力。
    /// </summary>
    public int? BatteryLevel { get; init; }
}
