namespace SmartLock.Backend;

/// <summary>
/// 一筆下發指令在後端追蹤生命週期中的狀態。
///
/// 合法的狀態轉移路徑(單向，不會逆向或跳躍)：
///
///   Pending --(收到 ACK)--------------> Accepted
///   Pending --(超過 TTL 仍未收到 ACK)--> Expired
///   Accepted --(收到成功的 result)-----> Executed
///   Accepted --(收到失敗的 result)-----> Failed
///   Accepted --(超過 TTL 仍未收到 result)--> Expired
///
/// 這裡刻意不用 Stateless 這類第三方狀態機函式庫，
/// 用手寫 enum + switch 表達轉移規則——對於這種只有 5 個狀態、
/// 轉移規則單純的場景，手寫版本一樣清楚，且不引入額外依賴。
/// </summary>
public enum CommandState
{
    Pending,
    Accepted,
    Executed,
    Failed,
    Expired
}
