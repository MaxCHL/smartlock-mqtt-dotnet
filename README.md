# SmartLock MQTT Sample

智慧門鎖 MQTT 系統的技術展示範例，用 .NET 10 (LTS) + MQTTnet 5.2 實作(MQTTnet 5 預設改用 MQTT 5.0，本範例明確固定為 MQTT 3.1.1)，重點在展現
**指令下發/確認/執行的可靠性設計**（idempotency、TTL、狀態機、上下線偵測），
而不是做成完整產品。

## 系統架構圖

完整的視覺化架構圖（四層架構、Topic 分層、指令生命週期時序、狀態機、上下線偵測、安全性）
放在 [`docs/architecture.html`](docs/architecture.html)。

> 專案開啟 GitHub Pages 後，可直接線上瀏覽：<https://maxchl.github.io/smartlock-mqtt-dotnet/architecture.html>
> （或把 `docs/architecture.html` 下載後用瀏覽器開啟。）

## 架構角色

```
SmartLock.Contracts   共用契約：LockCommand / CommandAck / CommandResult / DeviceStatus / Topics
SmartLock.Device      模擬門鎖裝置(console app)：接收指令、執行、回報結果、維護上下線狀態
SmartLock.Backend     後端服務(console app)：下發指令、追蹤狀態機、維護裝置在線狀態
mosquitto             本機 demo 用的 MQTT broker(docker-compose)
```

### Topic 分層設計

| Topic | 方向 | 說明 |
|---|---|---|
| `locks/{deviceId}/command` | 後端 → 裝置 | 下發開關鎖指令 |
| `locks/{deviceId}/command/ack` | 裝置 → 後端 | 指令已收到(尚未執行完成) |
| `locks/{deviceId}/command/result` | 裝置 → 後端 | 指令執行結果 |
| `locks/{deviceId}/status` | 裝置 → 後端 | 上下線狀態(retained + Last Will) |

分層定義在 `SmartLock.Contracts/Topics.cs`，正式環境可以搭配 broker ACL，
限制每個裝置憑證只能存取自己 `locks/{deviceId}/*` 底下的 topic。

## 關鍵設計點對照

| 設計點 | 位置 |
|---|---|
| Idempotency(冪等性)檢查，避免重複開鎖 | `SmartLock.Device/DeviceSimulator.cs` 的 `HandleCommandAsync`，用 `_processedCommands`(`ConcurrentDictionary<Guid, CommandResult>`) |
| TTL 過期判斷 | `SmartLock.Contracts/LockCommand.cs` 的 `IsExpired`；裝置端在 `HandleCommandAsync` 檢查、後端在 `CommandTracker.CheckExpirations` 背景檢查 |
| Last Will(遺囑)+ retained 上下線狀態 | `SmartLock.Device/DeviceSimulator.cs` 的 `BuildClientOptions`(Will) 與 `PublishStatusAsync`(retained online) |
| 指令狀態機(Pending → Accepted → Executed/Failed，逾時 → Expired) | `SmartLock.Backend/CommandState.cs` + `CommandTracker.cs`(手寫 enum + switch，見下方說明) |
| Topic 分層命名 | `SmartLock.Contracts/Topics.cs` |
| 斷線自動重連 | `DeviceSimulator.OnDisconnectedAsync` / `BackendService.OnDisconnectedAsync`，範例用固定延遲重試 |

### 為什麼狀態機用手寫 enum + switch，不用 Stateless

範例只有 5 個狀態、轉移規則單純（見 `CommandState.cs` 的註解），手寫版本一樣清楚、
不需要額外依賴，也避免在沒有實機驗證 NuGet 套件版本相容性的情況下引入風險。
`CommandTracker.TryTransition` 用 switch expression 表達合法轉移，不合法的轉移
(例如在 Pending 就收到執行結果)會被直接忽略並記錄警告；重複的終態結果(Executed/Failed，例如重送觸發裝置冪等快取)
只記錄為重複結果並忽略；Accepted 狀態下又收到 ACK 則記錄為重複 ACK 並拒絕轉移，都不會讓狀態機被撥回或跳躍。
每筆指令的「檢查 + 轉移」以該指令為單位上鎖，MQTT 回呼與 TTL 計時器並行時也只會套用一次終態轉移。

## 如何跑起來

### 環境需求

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)(LTS，支援至 2028-11-14；專案 `TargetFramework` 為 `net10.0`)
- Docker(用來跑本機 mosquitto broker)

### 1. 啟動本機 broker

```bash
docker compose up -d
```

會啟動 `eclipse-mosquitto:2`，監聽 `localhost:1883`，允許匿名連線(僅供 demo，見下方安全性章節)。

### 2. 啟動裝置端(模擬門鎖)

```bash
dotnet run --project SmartLock.Device -- lock-001
```

參數是 deviceId，預設 `lock-001`。也可以用環境變數覆寫 broker 位置：
`MQTT_BROKER_HOST`、`MQTT_BROKER_PORT`(預設 `localhost` / `1883`)。

### 3. 啟動後端服務

另開一個終端機：

```bash
dotnet run --project SmartLock.Backend -- lock-001
```

參數是要下發指令的目標 deviceId，需與裝置端一致。啟動後會自動送出一筆 `Unlock` 指令，
並顯示互動選單：

```
指令: [u] 送出 Unlock  [l] 送出 Lock  [r] 重送上一筆指令(demo idempotency)  [q] 離開
```

輸入 `r` 可以示範 idempotency：後端用**同一個 CommandId**重送上一筆指令，裝置端會判斷
「這筆指令已經處理過」，直接回傳先前的結果，不會重新開鎖。

## 預期的 demo log 輸出

裝置端(節錄)：

```
=== SmartLock.Device：模擬智慧門鎖裝置 ===
DeviceId    : lock-001
Broker      : localhost:1883
[Device:lock-001] 連線到 localhost:1883 ...
[Device:lock-001] 已連線。訂閱指令 topic 並發佈上線狀態。
[Device:lock-001] 執行指令 3f2a...: Unlock ...
[Device:lock-001] 指令 3f2a... 執行完成: Unlock 成功。
[Device:lock-001] 指令 3f2a... 先前已處理過(idempotent)，直接回傳快取結果，不重複開鎖。
```

後端(節錄)：

```
=== SmartLock.Backend：指令派送與狀態追蹤 ===
TargetDeviceId : lock-001
Broker         : localhost:1883
[Backend] 已連線，訂閱 ACK / 執行結果 / 狀態 topic。
[Backend] [lock-001] 狀態更新: 上線，lastSeen(UTC)=2026-08-28T...
[Backend] [lock-001] 指令 3f2a... (Unlock) 狀態轉移: (新建) -> Pending  | 指令已下發
[Backend] [lock-001] 指令 3f2a... (Unlock) 狀態轉移: Pending -> Accepted  | 收到裝置 ACK
[Backend] [lock-001] 指令 3f2a... (Unlock) 狀態轉移: Accepted -> Executed  | 裝置回報執行成功
[Backend] 指令 3f2a... 收到重複結果 (Executed)，依冪等原則忽略。
```

把裝置端(Ctrl+C，行程直接結束、不送 DISCONNECT)關掉後，後端會看到 Last Will 觸發的離線狀態：

```
[Backend] [lock-001] 狀態更新: 離線，lastSeen(UTC)=2026-08-28T...
```

## 測試

```bash
dotnet test
```

請在 repo 根目錄執行 `dotnet test`(`global.json` 會選用 Microsoft.Testing.Platform)；IDE 需啟用 MTP 支援，或改用專案內附的 VSTest adapter。

`SmartLock.Tests`(xUnit v3，經 Microsoft.Testing.Platform 執行；根目錄 `global.json` 已啟用 `dotnet test` 的此模式)涵蓋：

- `CommandTracker`：合法狀態轉移、不合法轉移被拒、重複 Executed/Failed 視為冪等、重複 ACK 被拒、結果與 TTL 逾時並行時只有一個終態、TTL 逾時轉 Expired。
- 裝置端 idempotency：同一個 CommandId 送兩次，只會執行(ACK)一次。
- 整合測試：Last Will 觸發離線、broker 重啟後裝置自動重連並回報上線、指令完整往返與重送(確認裝置沒有重新執行)、乾淨斷線不觸發 Last Will、對不在線裝置下發指令後逾時 Expired。

整合測試用 `MQTTnet.Server` 在測試行程內啟動 broker(隨機埠號，僅測試專案引用)，不需要 Docker 或 mosquitto。
重連測試受固定 5 秒重連延遲影響，約需 6 秒；整個測試約 8 秒。每個測試都會釋放自己建立的 client(`await using`)，不會有殘留的重連嘗試。

## 授權

MIT，詳見 [LICENSE](LICENSE)。

## 範例 vs 正式環境差異對照

| 面向 | 本範例 | 正式環境 |
|---|---|---|
| Idempotency / lastSeen 儲存 | `ConcurrentDictionary`(單一行程記憶體) | Redis(或其他外部共享儲存)，多實例部署才能共享、行程重啟也不會遺失 |
| 傳輸安全 | 明文 TCP 1883、匿名連線 | TLS(8883)+ mTLS(裝置憑證) + broker ACL(每個裝置只能存取自己的 topic) + 憑證輪替 |
| Broker | 單一 `eclipse-mosquitto` 容器 | Broker 叢集(例如 EMQX / VerneMQ 叢集，或雲端代管 MQTT 服務)，具備高可用與水平擴展能力 |
| 斷線重連 | 固定延遲重試(範例層級) | 自行實作(或採用社群套件)的託管重連層：指數退避重連、離線訊息佇列(MQTTnet 5 沒有 `ManagedMqttClient`，4.x 的 `MQTTnet.Extensions.ManagedClient` 僅停在 4.3.7，不適用 5.x) |
| 後端事件處理 | Console app 直接處理收到的訊息 | 當裝置數量與事件量變大時，Device Service 收到的 ACK/Result/Status 應該先落地到 Kafka/RabbitMQ 這類訊息佇列，解耦「接收 MQTT 事件」與「後續業務處理(狀態機、通知、資料庫寫入)」，避免業務處理變慢時回壓(back-pressure)到 MQTT 訂閱端造成訊息堆積 |
| 設定管理 | 環境變數 + 程式內預設值 | appsettings.json / K8s ConfigMap + Secret 管理，並有集中的憑證輪替機制 |

## 安全性注意事項(本範例刻意簡化)

- 本範例的 broker 允許匿名連線、走明文 1883，**只適合本機 demo**。
- `SmartLock.Device/DeviceSimulator.cs` 的 `BuildClientOptions` 註解列出正式環境要補的項目：
  TLS、mTLS(裝置憑證)、broker ACL、憑證輪替。
- 沒有在任何地方寫死真實密碼或憑證。
