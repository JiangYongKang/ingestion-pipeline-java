# 可插拔数据接入与同步管线（ingestion-pipeline-java）

一个基于 Spring Boot 3 / Java 21 的本地可运行数据接入服务：用**本地文件目录**和**本地 HTTP 端点**模拟源端，
支持推送式与拉取式两种接入，保证重复投递、断点恢复、结构演进后的**最终数据状态一致且可解释**。

所有状态（数据、位点、结构、隔离记录、指标、来源注册信息）都持久化在本地数据目录，
不依赖外部数据库或消息中间件。

---

## 1. 能力总览

| 需求 | 实现方式 | 可观测证据 |
|---|---|---|
| 多来源类型可插拔 | `PUSH`（HTTP 推送）、`PULL_FILE`（本地文件）、`PULL_HTTP`（HTTP 拉取，含内置模拟源端），工厂按配置装配 | 未知类型 `SOURCE_TYPE_UNKNOWN`，配置缺失 `SOURCE_CONFIG_INVALID` |
| 幂等写入 | 业务键 + 规范化内容 SHA-256；相同内容 `DUPLICATE`，不同内容 `CONFLICT`（不覆盖、保留证据） | 写入计数、`conflicts.jsonl` |
| 位点持久化与恢复 | 每来源位点快照（原子写）+ 单调/连续/版本 CAS 校验；重启后从已确认位点继续 | `offsets/<id>.json`、`OFFSET_FALLBACK` / `OFFSET_NON_MONOTONIC` / `OFFSET_VERSION_STALE` |
| 结构演进 | 受控版本化 schema：新增列必须声明确定默认值；删除列留痕；禁止改键列/改列类型 | schema 版本号、`SCHEMA_*` 隔离原因码 |
| 鉴权与来源隔离 | 数据面 Bearer 令牌按来源归属校验；管理面独立管理员令牌 | 401 `AUTH_MISSING/INVALID_CREDENTIALS`、403 `AUTH_FORBIDDEN` |
| 有界积压与失败隔离 | 每来源在途记录许可（`REJECT`/`RATE_LIMIT`）；坏数据逐条进死信队列，不中断整批 | 429 `BACKLOG_LIMIT_REACHED`、`quarantine/<id>.json`、尝试次数与 `exhausted` |
| 可观测可复现 | 每来源 received/inserted/duplicates/conflicts/quarantined/rejected/retried 持久计数 | `GET /admin/.../metrics`、`GET /admin/metrics` |

---

## 2. 接入协议

### 2.1 推送式（PUSH）

```
POST /api/sources/{sourceId}/records
Authorization: Bearer <sourceToken>
Content-Type: application/json

{
  "records": [
    {"offset": 1, "payload": {"id": "u1", "name": "alice", "age": 30}},
    {"offset": 2, "payload": {"id": "u2", "name": "bob",   "age": 31}}
  ]
}
```

- `offset`：来源侧单调递增正整数，**在来源内必须连续**。
- `payload`：记录 JSON 对象，按来源当前 schema 校验。
- 单批记录数受 `ingestion.max-batch-size` 限制，超限返回 `BATCH_TOO_LARGE`(400)。

**位点规则（关键）**

设当前已确认位点为 `C`，本批最小/最大位点为 `min/max`：

| 情况 | 判定 | 结果 |
|---|---|---|
| `min == C+1` 且批内连续 | 正常推进 | 处理后提交到 `max` |
| `max <= C` | **整批重放** | 幂等重放：全部命中 DUPLICATE / 复现 CONFLICT，位点不变，不产生重复数据 |
| `min <= C < max` | 部分重叠 | 409 `OFFSET_FALLBACK`（防止回退/错位） |
| `min > C+1` | 跳号 | 409 `OFFSET_NON_MONOTONIC` |
| 批内位点不连续或缺号 | 跳变 | 409 `OFFSET_NON_MONOTONIC` |

位点推进与数据写入在**同一把来源锁**内完成，并带版本号 CAS；
多个并发请求中只有连续分区成功，其余被位点规则拒绝（见并发测试），不会跳变、越位或数据错位。

**处理结果**

```json
{
  "sourceId": "s1",
  "fromOffset": 0,
  "committedOffset": 2,
  "received": 2,
  "inserted": 2,
  "duplicates": 0,
  "conflicts": 0,
  "quarantined": 0,
  "failures": []
}
```

### 2.2 拉取式（PULL_FILE）

- 配置 `watchDir`，在该目录投放形如 `000001.json`、`000002.json` 的文件（文件名为正整数位点）。
- 调度器（或手动 `POST /admin/pull/tick`）读取严格大于已确认位点的文件，按位点升序处理。
- **只处理从 `C+1` 开始的连续前缀**；存在缺口时等待补齐，绝不越过缺口提交。
- 非数字文件名被忽略，不参与位点。

### 2.3 拉取式（PULL_HTTP）

连接器对配置的 `baseUrl` 发起：

```
GET {baseUrl}/records?after={lastCommittedOffset}
Authorization: Bearer <sourceToken>
```

响应：

```json
{"records": [{"offset": 12, "payload": {"id": "x", "name": "y"}}]}
```

本服务内置了一个模拟源端，便于本地端到端验证：

- `POST /mock/sources/{sourceId}/feed`（Bearer 令牌；body 为记录 JSON，源端自动分配位点）
- `GET  /mock/sources/{sourceId}/records?after=n`（拉取协议，带鉴权，401/403 语义真实）

因此注册一个 HTTP 拉取来源时，`baseUrl` 写 `http://127.0.0.1:<port>/mock/sources/<sourceId>` 即可。

### 2.4 错误响应

所有拒绝统一返回：

```json
{"code": "OFFSET_FALLBACK", "message": "partially overlapping batch refused ..."}
```

HTTP 状态映射：401（缺/错凭据）、403（越权）、400（参数/配置/结构）、
404（来源不存在）、409（位点/冲突/来源停用）、429（积压）。
错误类别前缀不同，**鉴权问题与参数问题可区分**。

---

## 3. 兼容规则、冲突规则与隔离

### 3.1 结构（Schema）

- 每个来源一份版本化 schema，`keyField` 指定业务键列；列类型支持 `STRING/LONG/DOUBLE/BOOLEAN`。
- 首次注册：`POST /admin/sources/{id}/schema`（版本置为 1）。
- 演进：`POST /admin/sources/{id}/schema/evolve`，版本自动 +1。
  - **新增列**：必须显式声明 `defaultValue`（确定且稳定的默认语义），否则拒绝 `SCHEMA_EVOLUTION_INVALID`。
  - **删除列**：允许（键列除外）；删除事实留痕，用于识别仍携带该列的旧数据。
  - **改变既有列类型 / 更换键列**：禁止（会破坏对已存数据的解释）。
- 读取旧记录时，缺失的新增列**始终按列定义的默认值补值**，不依赖写入时的环境状态。
- 未在 schema 中声明的多余字段被**确定性剔除**（不报错、不落库、不参与哈希），保证前向兼容。

### 3.2 单条记录判定

| 情况 | 原因码 | 处理 |
|---|---|---|
| 不是合法 JSON / 不是对象 | `DATA_INVALID` | 隔离 |
| 业务键缺失 | `SCHEMA_MISSING_FIELD` | 隔离 |
| 携带已删除列 | `SCHEMA_REMOVED_COLUMN` | 隔离（防止错列/语义漂移，优先判定） |
| 必填列缺失 | `SCHEMA_MISSING_FIELD` | 隔离 |
| 类型不匹配（字符串与数值/布尔之间不做隐式转换） | `SCHEMA_TYPE_MISMATCH` | 隔离 |
| 可选列缺失 | — | 按默认值填充后正常写入 |

被隔离的记录：
- **不中断整批**：同批其它好记录正常处理；
- 位点仍越过该条（避免无限重投），但坏数据不进入数据存储、不污染已有状态；
- 原始 JSON、归类错误码、可读原因、首次/末次失败时间、尝试次数都保存在 `quarantine/<id>.json`。

### 3.3 冲突（同键不同内容）

- 业务键相同 + 规范化内容哈希相同 ⇒ `DUPLICATE`（字段顺序不同也判同）。
- 业务键相同但哈希不同 ⇒ `CONFLICT`：
  - **既不静默覆盖，也不丢弃**；
  - 既有记录保持不变，传入版本作为证据追加到 `data/<id>/conflicts.jsonl`（含双方哈希、位点、时间）；
  - 同一冲突证据去重，整批重放不会重复记账。

### 3.4 死信重试（受控，不无限重投）

```
POST /admin/sources/{id}/quarantine/{businessKey}/retry
Authorization: Bearer <adminToken>
Content-Type: application/json

（可省略 body：按原始内容重放；也可放修复后的记录 JSON 作为运维修复）
```

- 每次失败尝试 `attempts+1`；达到 `ingestion.max-retry-attempts` 后 `exhausted=true`，再次重试返回 400。
- 重试成功：记录正常幂等写入并移出奇队列；**位点不变**（该位点此前已确认）。

---

## 4. 鉴权与来源隔离

- 数据面所有 `/api/**` 请求必须带 `Authorization: Bearer <sourceToken>`：
  - 无头/非 Bearer ⇒ 401 `AUTH_MISSING_CREDENTIALS`；
  - 令牌不属于任何来源 ⇒ 401 `AUTH_INVALID_CREDENTIALS`；
  - 令牌有效但属于**其它来源**（跨来源访问）⇒ 403 `AUTH_FORBIDDEN`。
- 管理面 `/admin/**` 使用独立令牌 `ingestion.admin-token`（为空则全部拒绝）。
- **任何鉴权失败都在写入前返回：不产生数据写入，也不改变位点。**
- `rejected` 计数仅为可观测统计。

---

## 5. 有界积压、限速与资源边界

- 每来源维护 `ingestion.backlog-limit` 个“已准入未处理完”记录许可（公平信号量）：
  - `REJECT`（默认）：配额不足立即 429；
  - `RATE_LIMIT`：在 `ingestion.backlog-await-millis` 内等待空位，超时再 429（确定性限速）。
- 单批超过积压上限直接拒绝；超过 `ingestion.max-batch-size` 返回 400。
- 拉取每轮最多处理一个批大小的**连续前缀**，不会把无界数据一次性载入内存。
- 位点/快照等关键文件采用“临时文件 + 原子 move”，进程崩溃不会留下半截状态。

---

## 6. 配置项

| 配置 | 默认值 | 说明 |
|---|---|---|
| `ingestion.data-dir` | `data/ingestion` | 持久化根目录 |
| `ingestion.max-batch-size` | `100` | 单批最大记录数（超限 400） |
| `ingestion.backlog-limit` | `200` | 每来源在途记录积压上限 |
| `ingestion.backlog-policy` | `REJECT` | `REJECT` 或 `RATE_LIMIT` |
| `ingestion.backlog-await-millis` | `2000` | RATE_LIMIT 下的等待上限 |
| `ingestion.max-retry-attempts` | `3` | 死信最大尝试次数，之后不再自动重投 |
| `ingestion.pull-scheduler-enabled` | `true` | 是否启用拉取调度器（关闭后仍可手动 tick） |
| `ingestion.pull-interval-millis` | `5000` | 拉取间隔 |
| `ingestion.admin-token` | `dev-admin-token` | 管理令牌（生产用 `INGESTION_ADMIN_TOKEN` 覆盖） |
| `ingestion.sources[i].*` | — | 启动装配的来源（也可运行时通过管理端点注册并持久化） |

来源字段：`id`、`type`(`PUSH/PULL_FILE/PULL_HTTP`)、`token`（必填）、
`watchDir`(PULL_FILE 必填)、`baseUrl`(PULL_HTTP 必填)、`keyField`、`enabled`。

### 持久化目录布局

```
<data-dir>/
  sources.json                 # 动态注册的来源（重启恢复装配）
  offsets/<sourceId>.json      # 位点快照 + 版本号
  schemas/<sourceId>.json      # 当前 schema + 历史版本 + 已删除列
  data/<sourceId>/
      snapshot.json            # 压缩后的键值视图
      data.jsonl               # 追加式 WAL（快照后增量）
      conflicts.jsonl          # 冲突证据
  quarantine/<sourceId>.json   # 死信队列（原始错误 + 归类 + 尝试次数）
  metrics/<sourceId>.json      # 持久计数
```

---

## 7. 本地验证方法

### 7.1 运行测试（覆盖需求中的各场景）

```bash
mvn test
```

测试类与覆盖场景：

| 测试类 | 覆盖 |
|---|---|
| `IdempotencyAndOffsetTest` | 重复投递、整批重放幂等、位点回退/部分重叠/跳号拒绝、同键不同内容冲突 |
| `SchemaEvolutionTest` | 新增列默认值、非法演进拒绝、缺字段/类型不匹配/删除列隔离、修复重试、坏数据不污染 |
| `AuthAndConfigTest` | 缺凭据/错凭据/跨来源、鉴权与参数错误区分、未知类型/不完整配置、管理令牌 |
| `ConcurrencyAndBacklogTest` | 12 线程并发分区提交的一致性、积压 429、批量上限、计数可观测 |
| `PullSourceTest` | PULL_FILE 连续消费、PULL_HTTP 模拟源端拉取与来源侧鉴权、增量不重复 |
| `RecoveryAndMetricsTest` | 丢弃内存对象后用同一数据目录“重启”，位点/数据/指标恢复，重放幂等 |

测试日志使用 `[INPUT] / [JUDGE] / [STATE] / [QUARANTINE] / [CONCUR] / [OBSERVABLE]`
前缀打印**输入内容与判定依据**，可在 `target/surefire-reports/` 中查看。

### 7.2 手工端到端（推送）

```bash
mvn spring-boot:run
# 另开终端（默认管理令牌 dev-admin-token）
ADMIN="Authorization: Bearer dev-admin-token"
# 1) 注册来源
curl -s -X POST localhost:8080/admin/sources -H "$ADMIN" -H 'Content-Type: application/json' \
  -d '{"id":"s1","type":"PUSH","token":"push-secret"}'
# 2) 注册 schema
curl -s -X POST localhost:8080/admin/sources/s1/schema -H "$ADMIN" -H 'Content-Type: application/json' \
  -d '{"keyField":"id","columns":[
        {"name":"id","type":"STRING","required":true},
        {"name":"name","type":"STRING","required":true},
        {"name":"age","type":"LONG","required":false,"defaultValue":"0"}]}'
# 3) 推送
curl -s -X POST localhost:8080/api/sources/s1/records \
  -H 'Authorization: Bearer push-secret' -H 'Content-Type: application/json' \
  -d '{"records":[{"offset":1,"payload":{"id":"u1","name":"alice","age":30}}]}'
# 4) 重放（观察 duplicates=1，committedOffset 不变）
curl -s -X POST localhost:8080/api/sources/s1/records \
  -H 'Authorization: Bearer push-secret' -H 'Content-Type: application/json' \
  -d '{"records":[{"offset":1,"payload":{"id":"u1","name":"alice","age":30}}]}'
# 5) 查询状态
curl -s localhost:8080/admin/sources/s1/metrics  -H "$ADMIN"
curl -s localhost:8080/admin/sources/s1/data     -H "$ADMIN"
curl -s localhost:8080/admin/sources/s1/offset   -H "$ADMIN"
```

### 7.3 手工端到端（HTTP 拉取 + 内置模拟源端）

```bash
curl -s -X POST localhost:8080/admin/sources -H "$ADMIN" -H 'Content-Type: application/json' \
  -d '{"id":"h1","type":"PULL_HTTP","token":"tok1",
       "baseUrl":"http://127.0.0.1:8080/mock/sources/h1"}'
curl -s -X POST localhost:8080/admin/sources/h1/schema -H "$ADMIN" -H 'Content-Type: application/json' \
  -d '{"keyField":"id","columns":[{"name":"id","type":"STRING","required":true},
                                  {"name":"name","type":"STRING","required":true}]}'
# 向“源端”投放
curl -s -X POST localhost:8080/mock/sources/h1/feed -H 'Authorization: Bearer tok1' \
  -H 'Content-Type: application/json' -d '{"id":"x","name":"alice"}'
# 触发一轮拉取（或等待调度器）
curl -s -X POST localhost:8080/admin/pull/tick -H "$ADMIN"
curl -s localhost:8080/admin/sources/h1/offset -H "$ADMIN"   # offset 应为 1
```

PULL_FILE 类似：注册 `watchDir`，向目录写 `000001.json ...`，再 `POST /admin/pull/tick`。

---

## 8. 主要代码结构

```
config/   PipelineProperties, SourceConfig, PipelineBeans
model/    SourceType, ErrorCode, ColumnType/ColumnDef, SchemaDefinition,
          WriteOutcome, BatchResult, SourceMetrics
error/    IngestionException(错误码+原因), RecordFailure(死信条目)
source/   SourceConnector 及 PUSH / FILE / HTTP 实现, SourceConnectorFactory,
          SourceRegistry, SourceRecord, PollBatch
store/    OffsetStore, DataStore(WAL+快照), SchemaStore, QuarantineStore,
          MetricsStore, JsonFiles, SchemaState/OffsetState/StoredRecord/ConflictRecord
core/     SchemaValidator(兼容/隔离规则), IngestionService(写入+位点编排),
          AuthService, BacklogGuard, PullRunner, PullScheduler
web/      IngestController(推送), AdminController(管理面),
          MockSourceController(模拟源端), GlobalExceptionAdvice(错误码↔HTTP)
dto/      IngestRequest/IngestItem, ErrorResponse
```
