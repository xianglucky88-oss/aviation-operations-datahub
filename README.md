# 航空航班与机场运行数据平台

采集航班计划、实际起降、登机口、行李转盘和运行公告，统一机场、航班和运行事件的生命周期。

## 数据工程链路

`source adapters -> transport -> raw landing -> schema normalization -> identity resolution -> quality control -> deduplication -> batch state -> structured storage -> replay`

## 模块职责

- `sources.py`：fixture 和 HTTP(S) JSON 数据源；认证只从运行环境读取。
- `models.py`：不可变领域记录和来源批次模型。
- `normalize.py`：字段标准化、类型转换、领域枚举校验和稳定哈希。
- `identity.py`：领域主键和去重键（旧 `record_key`/`flight_key`/`airport_key` 行为冻结），以及确定性规范化、可版本化的航司/航班号别名与代码共享规则、航班身份解析器和 TTL 缓存。
- `identity_store.py`：SQLite 身份映射库与可查询身份索引；可逆迁移、乐观并发写、人工决策、失败队列与回填/重解析恢复。
- `identity_service.py`：组合解析器与存储的应用服务，负责映射切换、缓存失效、人工确认/撤回和批量索引。
- `quality.py`：领域质量规则和拒绝原因。
- `storage.py`：SQLite 结构化表、幂等写入、批次、原始清单和拒绝记录。
- `pipeline.py`：只负责编排标准化、质量校验和存储。
- `application.py`：组装依赖、保存原始响应和管理资源生命周期。
- `cli.py`：fixture/HTTP 两种运行入口。
- `identity.py, lifecycle.py, quality.py`：领域事件、生命周期和后续扩展规则。

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

## 航班身份解析（identity 层）

旧的 `record_key`、`flight_key`、`airport_key` 三个函数保持原有字符串行为，历史数据含义不变。新增的身份层在其之上工作，输出形如 `FV2|<航司>|<航班号><后缀>|<航班日期>` 的 `canonical_flight_key`。

### 解析原则

1. **先确定性规范化**：大小写、空白、航班号前导零（可配置）、ISO 日期、机场三字母代码；机场代码只做校验，不参与“同一航班”的判断。
2. **再按显式规则合并**：只有版本化映射里的航司别名、航班号别名、代码共享规则才能让两个标识等价；不做字符串相似度合并，也不会因为 origin/destination 相同就认定同一航班。
3. **无法唯一证明就转人工**：缺航司、航司与航班号前缀冲突且无别名连接、机场非法、日期非法、同批一个 flight_id 被多家航司占用、规则合并后航段互相矛盾等，状态为 `invalid` 或 `ambiguous`，不产生 canonical key，只生成 `review_key`、候选冲突和依据（evidence）。
4. **结果可复现**：映射在加载时排序并计算 `bundle_hash`，别名图做循环检测；同一输入与映射迭代顺序无关，重复调用命中 TTL 缓存且结果一致。

### 映射文件格式

示例见 `fixtures/identity_mapping_v1.json`：

```json
{
  "mapping_version": "mapping-2026-11-v1",
  "collapse_leading_zeros": true,
  "airline_aliases": [
    {"rule_id": "AA-CA-001", "alias": "CCA", "canonical": "CA"}
  ],
  "flight_aliases": [
    {"rule_id": "FA-MU-001", "airline": "MU", "number": "7201",
     "canonical_airline": "MU", "canonical_number": "201"}
  ],
  "codeshare_rules": [
    {"rule_id": "CS-001", "group_id": "GRP-1", "airline": "CA",
     "number": "1001", "operating": true},
    {"rule_id": "CS-002", "group_id": "GRP-1", "airline": "ZH",
     "number": "5001", "operating": false}
  ]
}
```

每个代码共享组必须恰好声明一个 `operating` 航班；营销航班的 canonical 身份归到实际承运航班。映射版本不可变：同版本号但内容哈希不同会被拒绝；别名成环、重复 rule_id、一个别名指向两个 canonical 航司都在加载时失败。

### 运行

接入映射后，CLI 与应用接口会在不改变既有 accept/reject 语义的前提下额外写入身份索引（身份解析失败进入 `identity_failures` 重试队列，不拒绝领域记录）：

```bash
PYTHONPATH=src python3 -m aviation_operations.cli \
  --fixture fixtures/codeshare_batch.json \
  --database /tmp/aviation-operations-datahub.sqlite3 \
  --raw-dir /tmp/aviation-operations-datahub-raw \
  --identity-mapping fixtures/identity_mapping_v1.json
```

### 身份索引与恢复

身份数据存放在独立的一组表中（`mapping_bundles`、`mapping_rules`、`manual_decisions`、`identity_index`、`identity_conflicts`、`identity_history`、`identity_failures` 等），通过编号的可逆迁移创建；旧库没有身份表时读到的是明确的 `IdentityIndexUnavailable`，迁移只新增身份表，回滚只删除身份表，领域表（`records` 等）不受影响。

- 只读查询：`IdentityReader.get_by_canonical / search / conflicts / history / failures / manual_decisions`。
- 映射切换：新版本激活后，旧映射算出的索引行标记 `stale=1`，用 `IdentityService.reindex()` 重解析，canonical key 变化写入 `identity_history`。
- 人工确认/撤回：`confirm_identity` 把 `review_key`（含覆盖的 fingerprint）绑定到一个 FV2 key；`withdraw_identity` 撤回后相关行重新置为 stale，身份回到待人工状态。
- 失败恢复：`backfill()` 给历史 `records` 补身份，单条失败入队；`retry_failures()` 重试并回填。
- 并发：WAL + busy timeout + 库级写锁 + 单调 `identity_write_version` 乐观令牌，过期令牌写入抛出 `ConcurrentUpdateError`。

## 外部依赖类型与版本

- Python 3.11+
- pytest 8.3.x（开发/测试）
- SQLite 3.x（本地结构化存储）
- HTTP(S) JSON 数据源（生产接入时按具体来源协议配置）

## 验证

```bash
# 安装了 pytest 时（测试模块同时兼容 pytest 与 unittest）
PYTHONPATH=src python3 -m pytest --import-mode=importlib tests
# 仅有标准库时（隔离模式，不依赖 PYTHONPATH/pip）
python3 -I scripts/run_tests.py
PYTHONPATH=src python3 -m compileall -q src
```
