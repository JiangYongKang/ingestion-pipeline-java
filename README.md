# 可插拔数据接入与同步管线（Java / Spring Boot）

一个模拟"多源数据接入 → 幂等写入 → 位点恢复 → 结构兼容 → 鉴权隔离 → 失败隔离 → 可观测"全链路的 Spring Boot 服务。源端用**本地 JSONL 文件**与**本地 HTTP 端点**模拟，所有状态（数据、位点、结构、批次结果、DLQ、指标）原子落盘到 `${ingestion.state-dir}`，最终数据状态一致且可解释。

## 1. 架构总览

```
源端模拟                接口层               处理层                   存储层（原子落盘）
─────────────  ─────────────────  ─────────────────────────  ─────────────────────
推送源(外部) → POST /api/sources/{src}/ingest ┐
                                              ├→ AuthService        → 401/403/404/400 可区分
文件源(JSONL)→ FileSourceReader ┐             ├→ IngestionService   → 批次幂等(BatchIdempotencyStore)
                               ├→ PullCoordinator                  → 总容量(CapacityGovernor 跨来源共享)
HTTP 源(本地)→ HttpSourceReader┘   (每来源独立轮询/串行处理)        → 限速/有界积压(RateLimiter)
                                                                 → 结构校验(SchemaValidator)
                                                                 → 幂等写入(RecordSink)
                                                                 → 失败隔离(DeadLetterStore)
管理面/可观测 ← AdminController ←──────────────────────────────────  → 位点(OffsetStore)
                                                                 → 指标(MetricsStore)
```

每个"来源"对应一个独立命名空间，数据/位点/结构/DLQ/批次结果按来源分目录隔离；推送入口与管理入口都强制 X-API-Key 鉴权。

## 2. 接入协议

### 2.1 推送式接入（PUSH）

```
POST /api/sources/{source}/ingest
Header: X-API-Key: <该来源 token>
Content-Type: application/json
{
  "batchId": "b-001",                       // 必填，整批幂等键
  "schema": {                               // 可选；无结构的首必须带
    "version": 1,
    "fields": [
      {"name": "id",  "type": "STRING", "required": true},
      {"name": "qty", "type": "LONG",   "required": false}
    ]
  },
  "records": [
    {"key": "o1", "data": {"id": "o1", "qty": 10}}
  ]
}
```

响应（`RecordResult` 逐记录给出状态与判定依据）：

```json
{"batchId":"b-001","replayed":false,"accepted":1,"duplicate":0,"conflict":0,
 "quarantined":0,"records":[{"key":"o1","status":"ACCEPTED","reason":null,"detail":null}]}
```

### 2.2 拉取式接入（PULL_FILE / PULL_HTTP）

- **PULL_FILE**：读取 `dir` 下 `records-*.jsonl`（JSON Lines，每行 `{"key":"...","data":{...}}`），文件名升序、行号即位点（从 1 起连续）。有界分页读取，不整文件载入内存。
- **PULL_HTTP**：周期调用 `GET {url}/records?after={n}&limit={m}`（携带 `X-API-Key`），要求记录带连续 `sequence`（从 `after+1` 起）。
- 拉取来源初始结构通过 `POST /api/admin/sources/{src}/schema` 声明（规则同推送批次内声明）。

### 2.3 HTTP 源端模拟器（用于本地验证 PULL_HTTP）

```
POST /mock/sources/{source}/seed   (X-API-Key)   [{"key":"h1","data":{...}}, ...]
GET  /mock/sources/{source}/records?after=0&limit=100 (X-API-Key)
```

### 2.4 管理/观测接口（均需 X-API-Key，且按凭据所属来源隔离）

| 方法 | 路径 | 说明 |
|-----|------|------|
| GET  | `/api/admin/metrics` | 凭据归属来源的计数快照 |
| GET  | `/api/admin/sources/{src}/metrics` | 指定来源接收/写入/重复/冲突/隔离/拒绝/重试等 |
| GET  | `/api/admin/sources/{src}/offset` | 已确认位点 |
| GET  | `/api/admin/sources/{src}/data` | 最终数据状态（冲突保留首版） |
| GET  | `/api/admin/sources/{src}/dlq` | 失败记录（原始数据+错误+归类+次数+状态） |
| POST | `/api/admin/sources/{src}/dlq/retry` | 按策略重试一轮，返回 resolved/stillFailed/exhausted |
| POST | `/api/admin/sources/{src}/schema` | 声明/演进结构 |
| GET  | `/api/admin/sources/{src}/quota` | 当前生效配额（batchSize/rateLimitPerSec/maxInflight） |
| PUT  | `/api/admin/sources/{src}/quota` | 不重启热调整配额；非法值 400 `INVALID_QUOTA` 且配置不变 |
| GET  | `/api/admin/capacity` | 进程级总容量快照：总量/已用/公平份额/各来源占用（任何有效凭据） |

## 3. 幂等、冲突与失败隔离规则

### 3.1 幂等（重复投递/重试/整批重放不产生重复）

1. **批次级**：同 `batchId` 的重复请求直接返回首次结果，`replayed=true`，无任何写入/位点变更（结果落盘，重启后仍识别重放）。
2. **记录级**：以业务键 `key` 唯一；比较采用**按键名排序的规范化 JSON 的 SHA-256**。内容相同 → `DUPLICATE`（不写）；键不存在 → `ACCEPTED`。每来源串行化写入 + 原子落盘，并发下每键最终恰好一行。

### 3.2 冲突（同键不同内容）

同一业务键再次出现且内容哈希不同 → 记录级状态 `CONFLICT`（reason=`RECORD_CONFLICT`），**保留首次写入内容**（不静默覆盖、不丢弃），响应/日志给出 stored/incoming 两个哈希作为判定依据。

### 3.3 失败隔离（单条坏数据不中断整批）

校验失败的记录进入该来源 `dlq.json`，保留**原始数据、原始错误信息、归类原因、尝试次数、状态、时间戳**；同批次其余记录正常写入。状态机：`PENDING → RETRIED/RESOLVED/EXHAUSTED`。

## 4. 位点规则（断点恢复）

- 每来源一个单调游标，先处理后提交，原子写 `offset.json`；拉取每条（含坏行）成功/隔离后以 `expected=当前, next=+1` 提交。
- **回退**（next < 当前，或 expected ≠ 当前）→ `OFFSET_BACKWARD(409)`；**非连续**（拉取模式 next ≠ 当前+1）→ `OFFSET_GAP(409)`，两类原因可区分。
- 每来源一把锁串行提交；并发推进时竞争失败方以最新位点重试，**不跳变、不越位、不错位**。
- 源端出现序号缺口或不可用 → 该来源**冻结**（`OFFSET_FROZEN`），停在已确认位点，绝不跳过缺失数据；修复后可重新声明结构或用新批次继续（测试中经 `PullCoordinator.unfreeze` 解冻）。
- 坏行也占位点，保证行号与游标一一对应、重启后不错位。

## 5. 结构兼容与演进规则

| 场景 | 规则 | 失败归类 |
|------|------|---------|
| 首个结构 | 版本必须为 1 | `SCHEMA_VERSION_INVALID(422)` |
| 后续声明 | 版本只能恰好 **+1**（禁止跳版） | `SCHEMA_VERSION_INVALID` |
| 新增字段 | **必须可选**；缺失时稳定默认 `null`（不补魔术值） | 必填新字段 → `SCHEMA_VERSION_INVALID` |
| 字段删除 | 记入 dropped 并跨版本留痕；旧数据里再出现该列 | `COLUMN_DROPPED(422)` |
| 出现未声明列 | 不静默丢弃、不错列 | `UNKNOWN_FIELD(422)` |
| 必填缺失/显式 null | 隔离该记录 | `MISSING_FIELD(422)` |
| 类型不匹配 | 隔离该记录，detail 给出 expected/actual | `TYPE_MISMATCH(422)` |
| 已删除列重新加入 | 拒绝（默认值语义二义） | `SCHEMA_VERSION_INVALID` |

逻辑类型：`STRING / LONG / DOUBLE / BOOLEAN`。已写入的旧版数据原样保留。

## 6. 鉴权与来源隔离

所有数据/管理接口在**任何写入与位点变更之前**先鉴权，未授权请求零副作用：

| 情况 | reason | HTTP |
|------|--------|------|
| 未带 X-API-Key | `MISSING_CREDENTIAL` | 401 |
| 凭据不属于任何来源 | `INVALID_CREDENTIAL` | 401 |
| 凭据属于其他来源 | `CROSS_SOURCE_ACCESS` | 403 |
| 来源不存在 | `UNKNOWN_SOURCE` | 404 |
| 类型未知 | `UNKNOWN_SOURCE_TYPE` | 400 |
| 配置不完整 | `INCOMPLETE_CONFIG` | 400 |
| JSON 不可解析 | `MALFORMED_BODY` | 400 |
| 缺 batchId / 批次超限 | `MISSING_BATCH_ID(400)` / `BATCH_TOO_LARGE(413)` | |
| 在途积压满 / 超令牌桶 | `BACKLOG_FULL / RATE_LIMITED` | 429 |

鉴权类（401/403）与参数类（400）、冲突类（409）、数据类（422）、积压类（429）状态码与 reason 码都不同，调用方可明确区分。

## 7. 有界积压、限速与重试策略

> 生效时机：下列按来源限制（速率 / 在途 / 单批规模）对**推送与拉取来源在进程启动后立即生效**——`QuotaService` 构造时即用启动配置为所有来源播种限速/积压桶，无需先做一次运行期调整；运行期热调整只改变此后的请求。

- `max-inflight`：每来源在途批次上限，满则按策略拒绝（`BACKLOG_FULL`），绝不无限排队。
- `rate-limit-per-sec`：每来源令牌桶（容量=每秒配额，按时间补充）；拉取途中令牌耗尽则本轮结束等下次（`RATE_LIMITED`）。
- `batch-size`：单批记录上限，超限整批拒绝（无部分写入）。
- 内存有界：读取按 `batch-size` 分页；限速/积压状态每来源仅一个计数器+令牌桶；JSONL 逐行流式读取。
- `max-attempts`：单条记录总尝试次数（含首次）。坏数据不会无限重投：达到上限标记 `EXHAUSTED`，后续 retry 不再投递它；修复后成功则 `RESOLVED`。

### 7.1 进程级总容量治理（跨来源）

在按来源保护之上，还有一份**进程级总容量**（`CapacityGovernor`），推送与拉取共用：

- **同一份总池**：每次推送批次 / 每轮拉取处理前，先从全局池取 1 个在途许可（与按来源 `max-inflight` 同维度），处理结束（含异常）必释放。全局占用到顶后，新到处理**立即明确拒绝** `GLOBAL_CAPACITY_EXHAUSTED`(429)，不无限排队、不撑爆内存；任何来源都无法绕过。
- **公平分配（确定、可复现）**：每来源静态公平份额 `share = max(1, global-max-inflight / 来源数)`（启动时按注册来源数确定）。任何来源在全局池中的占用不得超过其份额；因为 `来源数 × share <= 总量`，份额之和不超过总量，所以**某个来源即使长期占满自己的份额，也挤不占其它来源的份额**——每个来源在任何时刻都保证拿得到自己的份额。
- **与按来源保护的兼容关系**：总容量是**额外一层**，不改变既有按来源语义。一次推送批次的判定顺序为：批次幂等（重放直接返回，不占容量不计数）→ 总容量 → 按来源在途（`BACKLOG_FULL`）→ 按来源限速（`RATE_LIMITED`）→ 结构/写入。拉取每轮同理（总容量 → 在途 → 逐条令牌）。
- **推送与拉取完全对等，启动即生效**：`QuotaService` 在进程启动（Spring 单例构造）时，按 `application.properties` 的启动配置为**所有注册来源（PUSH / PULL_FILE / PULL_HTTP）统一播种**生效配额与限速/在途桶；`RateLimiter.register` 幂等（桶已存在则保留在途计数与令牌，绝不重置）。因此：
  - 进程刚启动、**尚未做任何运行期配额调整**时，推送来源与拉取来源一样立即遵守各自的 `rate-limit-per-sec` / `max-inflight` / `batch-size`，连续突发批次不会被直接接收；
  - 不存在"只有拉取来源受限、推送来源要等首次热调整才建桶"的按来源类型差异（这正是本版修复的启动期穿透缺口：此前推送桶只在首次 `PUT quota` 时才创建，无桶期间 `RateLimiter` 按"不限"放行）；
  - 任何一层（按来源速率 / 按来源在途 / 进程总容量）到边界，请求都被**明确拒绝**，原因码与细分指标互不相同；被拒请求不产生数据写入、不提交位点、不落批次结果，因此重试表现为一次新的真实拦截（计数随真实拦截次数累加），而**成功批次的整批重放**在容量扣减之前被识别并直接返回首次结果，拒绝计数不重复累加。
- **拒绝可解释**：指标中 `rejectedGlobalCapacity` / `rejectedBacklogFull` / `rejectedRateLimited` 三个细分计数分别对应"被总容量拦 / 被自身积压拦 / 被自身限速拦"，按来源可查（`GET /api/admin/sources/{src}/metrics`），且**计数与实际被拦次数一致**——整批重放在容量扣减之前被识别并直接返回首次结果，不会重复累加；全局快照（总量/已用/公平份额/各来源占用）见 `GET /api/admin/capacity`。

### 7.2 配额热调整（不重启）与生效时机

`PUT /api/admin/sources/{src}/quota` 可在运行期调整单来源的 `batchSize` / `rateLimitPerSec` / `maxInflight`（未给出的字段保持不变）。生效时机有明确边界：

- **启动配置就是第一份生效配额**：进程启动后、首次调整之前，所有来源（含推送）一律按 `application.properties` 的静态值受限，不存在无保护的启动窗口。
- **调整只对此后的请求生效**：已接收或正在处理的数据不受影响，不丢失、不重复、位点不错乱（在途计数保留；令牌补到新配额满桶，保证调大速率后立即生效；并发突发下各计数器/令牌桶均为同步原子扣减，不出现短时穿透）。
- 调整前后的生效配额可通过 `GET /api/admin/sources/{src}/quota` 查询对比。
- 非法配额（`batchSize<=0`、`rateLimitPerSec<0`、`maxInflight<=0`）整体拒绝 `INVALID_QUOTA`(400)，**当前生效配置保持不变**（先校验后写入，非法路径不触碰任何生效状态）。
- 重启后生效配置回到 `application.properties` 的静态值（热调整不落盘）。

### 7.3 容量/限速拒绝原因定义

| 触发边界 | reason | HTTP | 细分计数 | 副作用 |
|----------|--------|------|----------|--------|
| 单批记录数 > `batch-size` | `BATCH_TOO_LARGE` | 413 | 计入 `rejected` | 整批拒绝，无写入/无位点 |
| 来源自身令牌桶不足（超 `rate-limit-per-sec`） | `RATE_LIMITED` | 429 | `rejectedRateLimited` | 无写入/无位点/不落批次结果 |
| 来源自身在途数达 `max-inflight` | `BACKLOG_FULL` | 429 | `rejectedBacklogFull` | 无写入/无位点/不落批次结果 |
| 进程总在途到顶或超出本来源公平份额 | `GLOBAL_CAPACITY_EXHAUSTED` | 429 | `rejectedGlobalCapacity` | 无写入/无位点（拉取本轮放弃，数据留源端） |
| 非法热调整取值 | `INVALID_QUOTA` | 400 | 不计入容量类拒绝 | 生效配置不变 |

判定顺序（推送）：批次幂等 → 总容量 → 来源在途 → 来源速率 → 结构/写入；拉取每轮：总容量 → 来源在途 → 逐条令牌。因此同一请求只命中最外层那一个原因，三类容量拒绝原因稳定可区分。

## 8. 配置项（`application.properties`）

| 配置 | 默认 | 说明 |
|------|------|------|
| `ingestion.state-dir` | `./data` | 全部持久化状态根目录（按来源分子目录） |
| `ingestion.default-batch-size` | 100 | 默认单批上限 |
| `ingestion.default-max-inflight` | 1000 | 默认在途上限 |
| `ingestion.default-max-attempts` | 3 | 默认单条最大尝试次数 |
| `ingestion.default-poll-interval-ms` | 500 | 默认拉取轮询间隔 |
| `ingestion.pull.enabled` | true | 拉取调度总开关 |
| `ingestion.global-max-inflight` | 10000 | 进程级总容量：所有来源共享的在途许可总数；<=0 不启用。公平份额=max(1, 值/来源数) |
| `ingestion.sources[i].name` | — | 来源名（唯一） |
| `ingestion.sources[i].type` | — | `PUSH / PULL_FILE / PULL_HTTP` |
| `ingestion.sources[i].token` | — | 凭据（必填） |
| `ingestion.sources[i].dir` | — | PULL_FILE 必填：JSONL 目录 |
| `ingestion.sources[i].url` | — | PULL_HTTP 必填：端点基址 |
| `ingestion.sources[i].batch-size` | 默认 | 该来源批规模 |
| `ingestion.sources[i].rate-limit-per-sec` | 0=不限 | 该来源每秒令牌数 |
| `ingestion.sources[i].max-inflight` | 默认 | 该来源在途上限 |
| `ingestion.sources[i].poll-interval-ms` | 默认 | 该来源轮询间隔 |
| `ingestion.sources[i].max-attempts` | 默认 | 该来源单条最大尝试次数 |
| `ingestion.sources[i].enabled` | true | 设为 false 跳过该来源装配 |

装配期强校验：类型无法识别 → `UNKNOWN_SOURCE_TYPE`；名称/凭据/类型专属必需项缺失或重名 → `INCOMPLETE_CONFIG`（消息列出缺失项），应用启动即失败并给出原因。

## 9. 本地验证方法

```bash
# 1) 构建（JDK 21+）
mvn -q -DskipTests package

# 2) 启动（默认 8080，示例配置已含 orders-push / inventory-file，HTTP 源默认禁用）
java -jar target/ingestion-pipeline-java-1.0-SNAPSHOT.jar

# 3) 推送：声明结构 + 投递（再执行一遍即为整批重放，replayed=true）
curl -s -X POST http://127.0.0.1:8080/api/sources/orders-push/ingest \
  -H 'X-API-Key: push-token-orders' -H 'Content-Type: application/json' -d '{
    "batchId":"b-001",
    "schema":{"version":1,"fields":[
      {"name":"id","type":"STRING","required":true},
      {"name":"qty","type":"LONG","required":false}]},
    "records":[{"key":"o1","data":{"id":"o1","qty":10}}]}'

# 4) 文件拉取：先声明结构，再往目录追加 JSONL，观察位点自动推进
curl -s -X POST http://127.0.0.1:8080/api/admin/sources/inventory-file/schema \
  -H 'X-API-Key: file-token-inventory' -H 'Content-Type: application/json' \
  -d '{"version":1,"fields":[{"name":"sku","type":"STRING","required":true}]}'
printf '%s\n' '{"key":"s1","data":{"sku":"s1"}}' >> data/sample-file-source/records-001.jsonl
curl -s http://127.0.0.1:8080/api/admin/sources/inventory-file/offset \
  -H 'X-API-Key: file-token-inventory'

# 5) 观测 / DLQ / 重试
curl -s http://127.0.0.1:8080/api/admin/sources/orders-push/metrics -H 'X-API-Key: push-token-orders'
curl -s http://127.0.0.1:8080/api/admin/sources/orders-push/dlq    -H 'X-API-Key: push-token-orders'
curl -s -X POST http://127.0.0.1:8080/api/admin/sources/orders-push/dlq/retry -H 'X-API-Key: push-token-orders'

# 6) 容量治理：查看/热调整配额（立即生效、可查询对比；非法值 400 且配置不变）
curl -s http://127.0.0.1:8080/api/admin/sources/orders-push/quota -H 'X-API-Key: push-token-orders'
curl -s -X PUT http://127.0.0.1:8080/api/admin/sources/orders-push/quota \
  -H 'X-API-Key: push-token-orders' -H 'Content-Type: application/json' \
  -d '{"rateLimitPerSec": 200, "batchSize": 50}'
curl -s -X PUT http://127.0.0.1:8080/api/admin/sources/orders-push/quota \
  -H 'X-API-Key: push-token-orders' -H 'Content-Type: application/json' \
  -d '{"batchSize": 0}'   # -> 400 INVALID_QUOTA，生效配置不变

# 7) 进程级总容量快照（总量/已用/公平份额/各来源占用）
curl -s http://127.0.0.1:8080/api/admin/capacity -H 'X-API-Key: push-token-orders'

# 8) 启动期推送保护复现（关键：全新启动后不要先 PUT quota）
#    orders-push 启动配置 rate-limit-per-sec=50、max-inflight=200。
#    进程刚启动、未做任何运行期调整时，连续突发超过 50 条/s 的批次必须立即收到
#    429 RATE_LIMITED，而不是被直接接收；重启进程后行为一致（热调整不落盘）。
for i in $(seq 1 6); do
  curl -s -o /dev/null -w "burst #$i -> %{http_code}\n" \
    -X POST http://127.0.0.1:8080/api/sources/orders-push/ingest \
    -H 'X-API-Key: push-token-orders' -H 'Content-Type: application/json' \
    -d "{\"batchId\":\"startup-burst-$i-$RANDOM\",\"records\":[$(seq -s, 1 20 | sed 's/[0-9]*/{"key":"k'$i'-&","data":{"id":"x"}}/g')]}"
done
# 预期：前若干批 200（令牌桶容量内），桶耗尽后立即 429 {"reason":"RATE_LIMITED",...}；
#   GET metrics 中 rejectedRateLimited 与实际被拦次数一致，data 中无被拒批次的部分写入。
curl -s http://127.0.0.1:8080/api/admin/sources/orders-push/metrics -H 'X-API-Key: push-token-orders'
```

## 10. 测试

```bash
mvn test
```

共 33 个用例（测试日志以 `SCENARIO` logger 打印每个场景的 **GIVEN 输入 / EXPECT / ACTUAL 判定 / BECAUSE 依据**）：

| 测试类 | 覆盖场景 |
|--------|---------|
| `SourceAssemblyTest` | 类型未知 vs 配置不完整原因可区分、重名、未知来源 |
| `IdempotencyConflictTest` | 重复投递、整批重放、重试幂等、同键异内容冲突且保留原值 |
| `OffsetStoreTest` | 位点回退/缺口/陈旧 expected 拒绝、16 线程并发推进无跳变、重启恢复 |
| `SchemaEvolutionTest` | 新增字段默认 null、缺字段/类型不符/删列/未声明列四类隔离、版本只能+1、新字段禁必填 |
| `AuthIsolationTest` | 缺凭据/无效/跨来源/未知来源四类可区分 |
| `PullFileSourceTest` | 文件拉取位点推进、坏行进 DLQ 不中断、重启续读、有界分页、缺口冻结 |
| `PullHttpSourceTest` | HTTP 携凭据连续拉取、位点提交、源端未授权 401 |
| `ConcurrencyAndBacklogTest` | 12 线程并发写入每键一行无重复无丢失、积压满/限速原因可区分且零写入、批次超限 |
| `GlobalCapacityGovernanceTest` | 同进程推送+拉取共享同一总容量池、公平份额防止挤占、配额热调整立即生效且非法值被拒、拒绝零写入零位点变更、重放不累加拒绝计数、**启动零调整时推送+拉取均立即受限** |
| `PushStartupProtectionTest` | **进程启动后未做任何调整：推送速率突发边界（12 线程并发无穿透）、在途边界、单批规模立即生效；被拒批次重投按真实拦截计数、成功批次重放不重复计数** |
| `StartupProtectionWiringTest` | **真实 Spring 装配：上下文启动后推送/拉取桶均已按启动配置就位，推送速率边界在首次请求即生效（防"仅测试手工 register 才生效"回归）** |
| `AdminQuotaEndpointTest` | 管理端点：配额查询/热调整/非法拒绝/跨来源 403、总容量快照观测与鉴权 |
| `WebEndToEndTest` | 真实 HTTP：401/403 零写入、声明+投递+重放、指标、DLQ 有界重试、最终状态一致 |
| `ApplicationTests` | Spring 上下文装配冒烟 |

## 11. 落盘结构（可解释/可复现）

```
${ingestion.state-dir}/sources/{source}/
  offset.json     # 已确认位点
  data.json       # key -> {data, contentHash, schemaVersion}（冲突保留首版）
  schema.json     # 全部版本历史 + 累计 dropped 列
  dlq.json        # 失败记录（原始数据/错误/原因/attempts/status/时间戳）
  metrics.json    # 计数快照
  batches/{batchId}.json  # 每个 batchId 的首次处理结果
```
