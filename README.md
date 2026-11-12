# 航空航班与机场运行数据平台

采集航班计划、实际起降、登机口、行李转盘和运行公告，统一机场、航班和运行事件的生命周期。

## 数据工程链路

`source adapters -> transport -> raw landing -> schema normalization -> identity resolution -> quality control -> deduplication -> batch state -> structured storage -> replay`

## 模块职责

- `sources.py`：fixture 和 HTTP(S) JSON 数据源；认证只从运行环境读取。
- `models.py`：不可变领域记录和来源批次模型。
- `normalize.py`：字段标准化、类型转换、领域枚举校验和稳定哈希。
- `identity.py`：领域主键和去重键。
- `quality.py`：领域质量规则和拒绝原因。
- `storage.py`：SQLite 结构化表、幂等写入、批次、原始清单和拒绝记录。
- `pipeline.py`：只负责编排标准化、质量校验和存储。
- `application.py`：组装依赖、保存原始响应和管理资源生命周期。
- `cli.py`：fixture/HTTP 两种运行入口。
- `identity.py, lifecycle.py, quality.py`：领域事件、生命周期和后续扩展规则。
- `lifecycle_model.py`：单航班生命周期重建纯领域模型（状态轨迹 + 可配置一致性协议）。
- `lifecycle_store.py`：生命周期读模型的 SQLite 表、每航班锁、版本发布与恢复。

## 航班运行生命周期重建读模型

同一 `flight_id` + `flight_date` 的 `scheduled / boarding / delayed / departed /
arrived / cancelled` 观测按 `observed_at` 重建为可解释状态轨迹，每个状态采用都输出
`record_key`、来源、事件族（复用 `lifecycle.event_family`）、有效区间
（`effective_from/effective_to`，半开区间）、决策与冲突原因。

### 一致性协议（可配置、可审计）

两个冲突轴各自独立配置，取值均为 `reject` / `keep_conflict` / `source_priority`：

| 协议 | 同刻冲突 `same_timestamp_policy`（默认 `source_priority`） | 轨迹冲突 `transition_policy`（默认 `reject`） |
| --- | --- | --- |
| `reject` | 拒绝落败观测，仅写冲突审计 | 拒绝终态后/取消后/倒退状态，轨迹不变 |
| `keep_conflict` | 落败观测保留为 `adopted=false` 的冲突段，不占用有效区间 | 冲突观测保留为冲突段，与已采用轨迹并存 |
| `source_priority` | 按来源优先级（再按状态序、record_key）确定性裁决 | 仅当来源严格更高优先级时覆盖既有采用，并在段上记录覆盖原因 |

处理的场景：乱序快照、同刻不同来源、迟到的 departed/arrived、cancelled 后到达、
跨日期 `flight_date`、`actual_departure` 为空或格式不一致、重复修订与重放、
终态后异常观测、资源上限（`max_observations`）。完整配置经哈希生成
`config_fingerprint`，与输入指纹 `input_fingerprint` 一起写入每个版本。

### 增量、并发与崩溃恢复

- 重建只按 `flight_key` 查询 `flight_observations` 索引，不扫描全表。
- `flight_lifecycle_locks` 提供每航班咨询锁；同机重入可重入，跨进程冲突抛出
  `FlightLockConflict`，过期锁（持有者崩溃）按 TTL 回收，全部加/解锁写审计事件。
- 发布在单个 `BEGIN IMMEDIATE` 事务内完成：提交前崩溃无半成品，提交后崩溃版本完整；
  `recover_orphans()` 额外清理外部恢复文件中的孤儿行。
- 版本只增不改；相同（输入指纹 + 配置指纹）的重试不产生新版本，`force=True` 可强制发布。

### 迁移策略

迁移是幂等且附加式的：原 `records/batches/raw_manifests/rejected_records` 表与原始
数据格式完全不变；`schema_migrations` 记录版本，首次打开时自动把既有 `records`
快照回填到 `flight_observations`。老版本 CLI 平面参数（`--fixture/--url ...`）继续可用。

### 查询与导出

```bash
PYTHONPATH=src python3 -m aviation_operations.cli ingest \
  --fixture fixtures/lifecycle_synthetic.json \
  --database /tmp/aviation-operations-datahub.sqlite3

PYTHONPATH=src python3 -m aviation_operations.cli rebuild \
  --database /tmp/aviation-operations-datahub.sqlite3 \
  --flight-id CA200 --flight-date 2026-11-02 \
  --same-timestamp-policy source_priority \
  --transition-policy source_priority \
  --source-priority airport-a=5 --source-priority airline-feed=2

PYTHONPATH=src python3 -m aviation_operations.cli query \
  --database /tmp/aviation-operations-datahub.sqlite3 \
  --flight-id CA200 --flight-date 2026-11-02

PYTHONPATH=src python3 -m aviation_operations.cli export \
  --database /tmp/aviation-operations-datahub.sqlite3 \
  --flight-id CA200 --flight-date 2026-11-02
```

`export` 为轨迹中每个 `record_key` 附带原始载荷、来源与 `raw_hash` 溯源信息；
`LifecycleStore.verify_traceability()` 可校验每个事实都能回溯到来源记录。
范围仅限单航班生命周期，不包含机场资源排班或通用来源分页。

## 运行

```bash
PYTHONPATH=src python3 -m aviation_operations.cli \
  --fixture fixtures/sample.json \
  --database /tmp/aviation-operations-datahub.sqlite3 \
  --raw-dir /tmp/aviation-operations-datahub-raw
```

HTTP 模式：

```bash
PYTHONPATH=src python3 -m aviation_operations.cli \
  --url https://example.invalid/api/records \
  --database /tmp/aviation-operations-datahub.sqlite3 \
  --raw-dir /tmp/aviation-operations-datahub-raw
```

接口需要返回 JSON 数组，或包含 `records`、`data`、`items`、`results` 的对象；其他嵌套结构使用 `--records-key`。认证变量 `DATA_SOURCE_BEARER_TOKEN` 和 `DATA_SOURCE_API_KEY` 只作为环境变量名称示例，不写入真实值。

## 外部依赖类型与版本

- Python 3.11+
- pytest 8.3.x（开发/测试）
- SQLite 3.x（本地结构化存储）
- HTTP(S) JSON 数据源（生产接入时按具体来源协议配置）

## 验证

```bash
PYTHONPATH=src python3 -m pytest --import-mode=importlib tests
PYTHONPATH=src python3 -m compileall -q src
```
