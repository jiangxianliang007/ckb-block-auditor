# ckb-block-auditor

CKB Block Auditor（当前 crate 版本 `0.2.0`）。

它只通过 **CKB JSON-RPC** 审计**当前所连接节点看到的规范链区块**，输出单行 JSON 日志，方便接入日志采集或告警系统。

> 重要边界：这里的 `PASS` 只表示“**本工具当前已实现且适用的检查项**在本次审计尝试中全部通过”，**不是**对区块完成了 PoW、VM、cycles、since、maturity、完整 live-cell 历史、完整经济学重算后的“完整共识安全证明”。

---

## 1. 当前范围

### 已覆盖的大类

- 区块头/块体基础一致性
- 区块哈希与各类 merkle/hash 字段重算一致性
- proposal / transaction 重复项检查
- cellbase 结构检查
- 普通交易基础结构检查
- 输入源交易解析、输出索引检查
- 输出 occupied capacity 检查
- 普通交易容量守恒（仅限可明确分类为普通交易的情况）
- reward 目标块与金额一致性检查
- DAO 提现容量上限一致性检查（仅限当前代码能够明确追踪的情形）

### 明确**未**覆盖或不输出 PASS 的范围

以下字段在代码中仍保留，但当前不会作为“已实现通过项”出现在 PASS 日志中：

- `check_output_lock_hash_type`
- `check_input_historical_liveness`
- `check_pow`
- `check_expected_epoch_target`
- `check_two_phase_commit`
- `check_cellbase_maturity`
- `check_since`
- `check_extension_consensus_rules`
- `check_vm_scripts`
- `check_cycles`

其中 PoW、VM、cycles、since、maturity、two-phase commit、完整历史 live-cell 证明，本仓库当前都**没有扩展实现**。

reward / DAO 也仍然属于 **same-node RPC consistency check**：

- 使用同一个节点返回的 `get_consensus`
- `get_block_economic_state`
- `calculate_dao_maximum_withdraw`
- 以及区块 / 交易 / witness 数据

来检查这些数据彼此是否自洽；**不是**脱离节点 RPC 的完整独立共识重算器。

---

## 2. 运行方式

### 实时模式（不落盘 cursor）

启动时先审计当前 tip；之后只跟随新块。

> 注意：**不会**偷偷创建默认 cursor 文件。重启后也**不会**自动补回停机期间错过的区块。

```bash
cargo run -- \
  --rpc-url http://127.0.0.1:8114 \
  --node-id ckb-node-01 \
  --log-path ./audit.log \
  --poll-interval-ms 3000
```

### 持久化模式（显式指定 cursor）

文件不存在时，从当前 tip 开始审计并创建 cursor；
文件已存在时，从已保存进度继续，并补审停机期间漏掉的区块。

```bash
cargo run -- \
  --rpc-url https://mainnet.ckbapp.dev \
  --node-id ckb-mainnet-01 \
  --cursor-path ./data/mainnet.cursor.json \
  --log-path ./audit-mainnet.log \
  --poll-interval-ms 3000
```

### release 二进制

```bash
cargo build --release --locked
./target/release/ckb-block-auditor \
  --rpc-url https://mainnet.ckbapp.dev \
  --node-id ckb-mainnet-01 \
  --cursor-path ./data/mainnet.cursor.json \
  --log-path ./audit-mainnet.log
```

---

## 3. CLI / 环境变量（按当前代码实况）

| 参数 | 环境变量 | 默认值 | 当前代码中的实际作用 |
|---|---|---:|---|
| `--rpc-url` | `CKB_RPC_URL` | `http://127.0.0.1:8114` | CKB JSON-RPC 地址 |
| `--node-id` | `CKB_NODE_ID` | `unknown` | 直接写入日志 `node_id` |
| `--poll-interval-ms` | `CKB_POLL_INTERVAL_MS` | `3000` | 轮询间隔 |
| `--rpc-timeout-secs` | `CKB_RPC_TIMEOUT_SECS` | `10` | 单次 HTTP RPC 请求超时 |
| `--max-retries` | `CKB_MAX_RETRIES` | `2` | **额外重试次数**。`2` 表示“首次尝试 + 最多 2 次重试”，即最多 3 次 |
| `--cursor-path` | `CKB_CURSOR_PATH` | 无 | 显式启用持久化 cursor |
| `--log-path` | `CKB_LOG_PATH` | 无 | 输出文件；不填则 stdout |
| `--max-details` | `CKB_MAX_DETAILS` | `200` | 每条日志最多保留多少条 `details` |
| `--max-future-ms` | `CKB_MAX_FUTURE_MS` | `15000` | 时间戳检查允许的未来偏移上限 |
| `--median-time-span` | `CKB_MEDIAN_TIME_SPAN` | `11` | **当前代码会解析，但实际时间戳检查仍使用节点 `get_consensus.median_time_block_count`** |
| `--proposal-limit` | `CKB_PROPOSAL_LIMIT` | `1500` | **当前代码会解析，但实际 proposal 上限仍使用节点 `get_consensus.max_block_proposals_limit`** |
| `--tx-version` | `CKB_TX_VERSION` | `0` | **当前代码会解析，但实际交易 version 仍使用节点 `get_consensus.tx_version`** |
| `--history-retention` | `CKB_HISTORY_RETENTION` | `256` | cursor 中保留多少高度的历史哈希，用于有限深度重组回退 |
| `--dao-type-hash` | `CKB_DAO_TYPE_HASH` | 空字符串 | 可选一致性覆盖：若配置了，就要求它与节点 `get_consensus.dao_type_hash` 一致 |

### 当前固定退避策略

当前代码里退避不是 CLI 参数，而是固定实现：

- 第 1 次重试前等待 `100ms`
- 第 2 次重试前等待 `200ms`
- 第 3 次重试前等待 `400ms`
- 以此指数退避，最多放大到 64 倍基础间隔

### `max_retries` 的准确语义

`max_retries` 表示“**额外**重试次数”，不是总尝试次数。

以默认值 `2` 为例：

- 单个 HTTP / JSON-RPC 请求如果遇到 HTTP 非 2xx、RPC `error`、请求异常：
  - 最多尝试 `3` 次（1 次首次 + 2 次重试）
- 某个区块在一次完整审计后，如果发现“**必需数据返回 null / 丢失 / 暂不可验证**”这类**可恢复执行失败**：
  - 最多对这个高度重新审计 `3` 轮（1 次首次 + 2 次重试）

当前代码不会因为“规则明确失败”去重试把它刷成 PASS。

---

## 4. Cursor、重试、进度与重组行为

## 4.1 未设置 `--cursor-path`

- 不读取、不创建、不写入任何默认 cursor 文件
- 启动时先审计当前规范链 tip
- 同一进程内，如果 tip 区块审计执行失败（例如必需数据持续缺失），会在**内存里**记住待补审高度，下一轮**先重试这个失败高度**，不会因为 tip 前移就直接跳过
- 但如果进程重启，这个内存状态会丢失；重启后仍然只会从新的当前 tip 重新锚定

## 4.2 设置了 `--cursor-path`

- 文件不存在：从当前 tip 开始审计，然后创建 cursor
- 文件存在：从已保存进度继续，补齐停机期间漏掉的区块
- cursor 自动绑定 `genesis_hash`
- 同一条链上换 RPC，只要 genesis 一致，可以继续使用同一 cursor
- 不同链必须使用不同 cursor 文件
- 若 cursor 的 `genesis_hash` 与当前 RPC 的 genesis 不一致，会报错并且**不会覆盖原文件**

## 4.3 执行失败与“已完成进度”的区别

这是本次 PR 的核心改动之一：

- **规则 FAIL**：表示已经拿到足够数据，明确知道某条规则不通过。这样的区块算“**已完成审计**”，cursor 会继续前进。
- **执行 FAIL**：表示本次审计最终输出也是 `FAIL`，但失败原因是“必需数据缺失 / RPC 返回空 / 歧义无法解析 / 重试耗尽”，并**不等于**已经证明这个区块无效。这样的区块**不算完成审计**：
  - 不会跨过去推进“已完成游标”
  - 下一轮 / 下次启动（持久化 cursor 模式）会继续补审

## 4.4 重组 / 分支变化

- 已完成游标会保存最近一段高度的哈希历史，用来寻找公共祖先
- 如果规范链发生有限深度重组，会回退到公共祖先之后重新审计
- 对于“待补审高度”，cursor 还会保留 `next_height` / `next_hash`
- 如果补审时发现同一高度的当前规范块哈希和之前失败时记住的哈希不同，程序会把它当作**新分支上的新审计尝试**，不会把新旧分支数据混在同一条结论里

因此，README 不再声称“每个区块永远只有一条日志”。

**真实行为是：每次审计尝试 / 审计轮次输出一条 JSON。**

同一高度、甚至同一块哈希，都可能因为：

- 首次执行失败，后续补审恢复
- 同高度发生重组，切到新哈希

而出现多条日志。请通过下面这些字段关联：

- `block_height`
- `block_hash`
- `parent_hash`
- `timestamp`
- `canonical_at_audit`

程序不会覆盖旧日志。

---

## 5. schema v4 迁移说明

本次日志 schema 从 **v3 升到 v4**。

### 5.1 与 v3 的主要差异

- `schema_version: 3` → `4`
- 删除顶层 `coverage`
- 删除顶层 `unknown_checks`
- 删除 `PASS_WITHIN_SCOPE` / `INCOMPLETE`
- 最终 `result` 只保留：
  - `PASS`
  - `FAIL`
- 日志里实际输出的适用 `check_*` 状态也只保留：
  - `PASS`
  - `FAIL`
- `details` 中增加以下上下文字段（存在时输出，不存在就省略）：
  - `failure_kind`
  - `rpc_method`
  - `attempts`
  - `max_retries`
- `DetailItem` 的可选字段统一省略 `null`

### 5.2 `FAIL` 现在包含两类完全不同的语义

请务必区分：

1. **`failure_kind = VALIDATION_FAILED`**  
   已拿到足够数据，规则被明确证伪。它是“规则失败”。

2. **`failure_kind = RETRY_EXHAUSTED` / `EXECUTION_FAILED`**  
   本次审计执行失败：
   - `RETRY_EXHAUSTED`：对可恢复缺数已经做了有界重试，仍然不能完成验证
   - `EXECUTION_FAILED`：当前代码判断这类问题本身就不可恢复或不应继续重试（例如 DAO 歧义无法解析）

第二类 `FAIL` 是**运行/审计失败告警**，**不是**“已经证明区块无效”。

### 5.3 `failed_checks` 的含义

- `failed_checks` 会汇总**所有最终对外输出为 FAIL 的检查项**
- 即使 `details` 被截断，甚至 `max_details=0`，`failed_checks` 仍应完整
- 最终 `result` 不依赖保留下来的 `details` 数量，而依赖检查汇总本身

---

## 6. 日志字段说明

### 始终存在的主要字段

- `timestamp`：本次日志写出时间
- `schema_version`：当前为 `4`
- `service`：固定为 `ckb-block-auditor`
- `auditor_version`：当前程序版本
- `node_id`：CLI 传入的节点标识
- `block_height` / `block_hash` / `parent_hash` / `block_timestamp`
- `canonical_at_audit`：本次尝试开始时，这个高度当前规范链头是否仍是正在审计的这个块
- `result`：`PASS` 或 `FAIL`
- `audit_duration_ms`：本次审计尝试耗时

### 异常时可能出现的字段

- `failed_checks`
- `details_total`
- `details_truncated`
- `details`

### `details` 中的重要字段

- `check_name`：对应哪个 `check_*`
- `status`：最终输出只会是 `FAIL`
- `error_code`：原始错误码，尽量保持具体
- `failure_kind`：`VALIDATION_FAILED` / `RETRY_EXHAUSTED` / `EXECUTION_FAILED`
- `rpc_method`：相关 RPC（有的话）
- `attempts`：本条日志对应这次区块审计尝试一共跑了多少轮
- `max_retries`：本次配置的额外重试次数
- `tx_hash` / `tx_index` / `input_index` / `output_index` / `referenced_out_point`
- `expected_operator` / `expected_value` / `actual_value` / `unit`
- `reason`

---

## 7. 日志示例（与 schema v4 实际序列化一致）

### 7.1 全部已实现且适用检查通过：`PASS`

```json
{"timestamp":"2026-09-14T07:30:01.250Z","schema_version":4,"service":"ckb-block-auditor","auditor_version":"0.2.0","node_id":"ckb-node-01","block_height":2,"block_hash":"0xaaa...","parent_hash":"0xbbb...","block_timestamp":1726043401000,"canonical_at_audit":true,"result":"PASS","audit_duration_ms":12,"check_block_height":"PASS","check_parent_hash":"PASS","check_epoch_continuity":"PASS","check_timestamp":"PASS","check_block_size":"PASS","check_proposal_limit":"PASS","check_block_hash":"PASS","check_transaction_hashes":"PASS","check_transactions_root":"PASS","check_proposals_hash":"PASS","check_extra_hash":"PASS","check_duplicate_transactions":"PASS","check_duplicate_proposals":"PASS","check_cellbase_structure":"PASS","check_transaction_version":"PASS","check_inputs_outputs_structure":"PASS","check_outputs_data_length":"PASS","check_duplicate_cell_deps":"PASS","check_duplicate_header_deps":"PASS","check_duplicate_inputs_in_transaction":"PASS","check_duplicate_inputs_in_block":"PASS","check_input_content_resolution":"PASS","check_input_output_index":"PASS","check_occupied_capacity":"PASS","check_ordinary_capacity_conservation":"PASS","check_cellbase_reward_amount":"PASS","check_cellbase_reward_target":"PASS"}
```

### 7.2 明确规则失败：`FAIL` + `VALIDATION_FAILED`

```json
{"timestamp":"2026-09-14T07:31:01.250Z","schema_version":4,"service":"ckb-block-auditor","auditor_version":"0.2.0","node_id":"ckb-node-01","block_height":124,"block_hash":"0xccc...","parent_hash":"0xddd...","block_timestamp":1726043461000,"canonical_at_audit":false,"result":"FAIL","audit_duration_ms":18,"check_block_size":"FAIL","check_transaction_hashes":"FAIL","check_transactions_root":"FAIL","failed_checks":["check_block_size","check_transaction_hashes","check_transactions_root"],"details_truncated":false,"details_total":3,"details":[{"check_name":"check_block_size","status":"FAIL","error_code":"BLOCK_SIZE_EXCEEDED","failure_kind":"VALIDATION_FAILED","expected_operator":"less_than_or_equal","expected_value":"597000","actual_value":"600123","unit":"byte","reason":"block serialized size exceeds the consensus max_block_bytes limit"},{"check_name":"check_transaction_hashes","status":"FAIL","error_code":"TRANSACTION_HASH_MISMATCH","failure_kind":"VALIDATION_FAILED","tx_hash":"0xeee...","tx_index":1,"expected_operator":"equal","expected_value":"0xeee...","actual_value":"0xfff...","unit":"hash","reason":"recomputed transaction hash does not match the rpc-provided transaction hash"},{"check_name":"check_transactions_root","status":"FAIL","error_code":"TRANSACTIONS_ROOT_MISMATCH","failure_kind":"VALIDATION_FAILED","expected_operator":"equal","expected_value":"0x111...","actual_value":"0x222...","unit":"hash","reason":"recomputed transactions_root does not match the header field"}]}
```

### 7.3 必需数据多次重试后仍不可验证：`FAIL` + `RETRY_EXHAUSTED`

```json
{"timestamp":"2026-09-14T07:32:01.250Z","schema_version":4,"service":"ckb-block-auditor","auditor_version":"0.2.0","node_id":"ckb-node-01","block_height":20423295,"block_hash":"0x555...","parent_hash":"0x444...","block_timestamp":1789124255297,"canonical_at_audit":true,"result":"FAIL","audit_duration_ms":2416,"check_block_height":"PASS","check_parent_hash":"PASS","check_epoch_continuity":"PASS","check_timestamp":"PASS","check_block_size":"PASS","check_proposal_limit":"PASS","check_block_hash":"PASS","check_transaction_hashes":"PASS","check_transactions_root":"PASS","check_proposals_hash":"PASS","check_extra_hash":"PASS","check_duplicate_transactions":"PASS","check_duplicate_proposals":"PASS","check_cellbase_structure":"PASS","check_transaction_version":"PASS","check_inputs_outputs_structure":"PASS","check_outputs_data_length":"PASS","check_duplicate_cell_deps":"PASS","check_duplicate_header_deps":"PASS","check_duplicate_inputs_in_transaction":"PASS","check_duplicate_inputs_in_block":"PASS","check_input_content_resolution":"FAIL","check_input_output_index":"FAIL","check_occupied_capacity":"PASS","check_ordinary_capacity_conservation":"FAIL","check_cellbase_reward_amount":"PASS","check_cellbase_reward_target":"PASS","failed_checks":["check_input_content_resolution","check_input_output_index","check_ordinary_capacity_conservation"],"details_truncated":false,"details_total":3,"details":[{"check_name":"check_input_content_resolution","status":"FAIL","error_code":"INPUT_TX_MISSING","failure_kind":"RETRY_EXHAUSTED","rpc_method":"get_transaction","attempts":3,"max_retries":2,"tx_hash":"0x666...","tx_index":1,"input_index":0,"referenced_out_point":"0x777...:0","reason":"get_transaction returned null"},{"check_name":"check_input_output_index","status":"FAIL","error_code":"INPUT_INDEX_UNVERIFIED","failure_kind":"RETRY_EXHAUSTED","rpc_method":"get_transaction","attempts":3,"max_retries":2,"tx_hash":"0x666...","tx_index":1,"input_index":0,"referenced_out_point":"0x777...:0","reason":"input output index could not be verified because the source transaction was unavailable"},{"check_name":"check_ordinary_capacity_conservation","status":"FAIL","error_code":"ORDINARY_CAPACITY_CLASSIFICATION_UNKNOWN","failure_kind":"RETRY_EXHAUSTED","attempts":3,"max_retries":2,"reason":"transaction 0x666... has unresolved inputs or unknown consensus classification, so ordinary capacity cannot be validated"}]}
```

> 再强调一次：上面这种 `FAIL` 是“审计执行失败告警”，不是“已经证明该区块无效”。

---

## 8. 每个已实现 `check_*` 到底如何判定 PASS

下面只列**当前代码确实会输出并参与 PASS/FAIL 判定**的项目；未实现项不在这里冒充已验证。

### 8.1 区块头 / 块体

#### `check_block_height`

- **适用条件**：非 genesis 块
- **依赖**：`get_header(parent_hash)`
- **PASS 条件**：`block.header.number == parent.number + 1`
- **FAIL 错误码**：`BLOCK_NUMBER_MISMATCH`
- **比较口径**：`expected_operator = equal`
- **缺数据处理**：
  - `PARENT_HEADER_MISSING`：父块头返回 `null`，会按区块级有限重试处理
  - `PARENT_HEADER_UNAVAILABLE`：父块头 RPC 请求失败；单次 RPC 自己先按 `max_retries` 重试，耗尽后本轮记执行 FAIL

#### `check_parent_hash`

- **适用条件**：非 genesis 块；genesis 不输出该字段
- **依赖**：`get_header(parent_hash)`
- **PASS 条件**：把取回的父块头转成 core header，本地 `calc_header_hash()` 后，结果必须等于当前块头里的 `parent_hash`
- **FAIL 错误码**：`PARENT_HASH_MISMATCH`
- **缺数据处理**：同 `check_block_height`

#### `check_epoch_continuity`

- **适用条件**：非 genesis 块
- **依赖**：`get_header(parent_hash)`
- **PASS 条件**：当前代码调用 `epoch_continuity(parent_epoch, current_epoch)`，要求：
  - 当前 epoch 编码格式是 well-formed
  - 且满足 `parent.is_genesis() || current.is_successor_of(parent)`
- **FAIL 错误码**：`EPOCH_CONTINUITY_FAIL`
- **缺数据处理**：同 `check_block_height`

#### `check_timestamp`

- **适用条件**：
  - genesis：直接 PASS
  - 非 genesis：需要父块及更多祖先时间戳
- **依赖**：
  - `get_header(parent_hash)`
  - 沿着父链继续 `get_header(ancestor_hash)`
  - `get_consensus.median_time_block_count`
  - `--max-future-ms` / `CKB_MAX_FUTURE_MS`
- **PASS 条件**：
  1. 收集父块以及继续回溯得到的祖先时间戳
  2. 只要回溯过程完整，或者已经凑满 `median_time_block_count` 个时间戳，就排序取中位数
  3. 要求当前块时间戳满足：
     - `timestamp > median`
     - `timestamp <= now + max_future_ms`
- **FAIL 错误码**：`TIMESTAMP_OUT_OF_RANGE`
- **缺数据处理**：
  - `TIMESTAMP_MEDIAN_INCOMPLETE`：祖先时间戳数量不够，区块级有限重试
  - `TIMESTAMP_ANCESTOR_UNAVAILABLE`：祖先 header RPC 失败；RPC 请求级先重试，仍失败则本轮执行 FAIL

#### `check_block_size`

- **依赖**：`get_consensus.max_block_bytes`
- **实际口径**：`core_block.data().serialized_size_without_uncle_proposals()`
- **PASS 条件**：`actual_size <= max_block_bytes`
- **FAIL 错误码**：`BLOCK_SIZE_EXCEEDED`
- **缺数据处理**：如果连 `get_consensus` 都拿不到，当前轮询直接报前置错误，不虚构区块 PASS

#### `check_proposal_limit`

- **依赖**：`get_consensus.max_block_proposals_limit`
- **PASS 条件**：`block.proposals.len() <= proposal_limit`
- **FAIL 错误码**：`PROPOSAL_LIMIT_EXCEEDED`

#### `check_block_hash`

- **PASS 条件**：本地 `core_block.data().header().calc_header_hash()` 必须等于 RPC 给出的 `block.header.hash`
- **FAIL 错误码**：`BLOCK_HASH_MISMATCH`

#### `check_transaction_hashes`

- **PASS 条件**：本地 `calc_tx_hashes()` 后，逐笔结果都要与各自 `tx.hash` 一致
- **FAIL 错误码**：`TRANSACTION_HASH_MISMATCH`

#### `check_transactions_root`

- **PASS 条件**：`core_block.calc_transactions_root()` 等于 `header.transactions_root`
- **FAIL 错误码**：`TRANSACTIONS_ROOT_MISMATCH`

#### `check_proposals_hash`

- **PASS 条件**：`block_data.calc_proposals_hash()` 等于 `header.proposals_hash`
- **FAIL 错误码**：`PROPOSALS_HASH_MISMATCH`

#### `check_extra_hash`

- **PASS 条件**：`core_block.calc_extra_hash().extra_hash()` 等于 `header.extra_hash`
- **FAIL 错误码**：`EXTRA_HASH_MISMATCH`

#### `check_duplicate_transactions`

- **PASS 条件**：区块内 `tx.hash` 全部唯一
- **FAIL 错误码**：`DUPLICATE_TRANSACTION`

#### `check_duplicate_proposals`

- **PASS 条件**：区块内 `proposal_short_id` 全部唯一
- **FAIL 错误码**：`DUPLICATE_PROPOSAL`

### 8.2 Cellbase

#### `check_cellbase_structure`

- **依赖**：仅区块本身数据
- **PASS 条件**：
  1. 区块必须有 index 0 交易，且它必须是 cellbase
  2. 后续交易里不能再出现 cellbase
  3. cellbase `outputs.len() <= 1`
  4. `outputs.len() == outputs_data.len()`
  5. 必须有且只有 1 个 input
  6. 该 input 的 `previous_output` 必须是 null outpoint
  7. 该 input 的 `since` 必须等于当前 `block_number`
  8. 必须有且只有 1 个 witness
  9. witness 必须能解码成 `CellbaseWitness`
  10. 如果有第一个 output，它的 `type` 必须为空
  11. 如果有第一个 `outputs_data`，它必须是空字节串
- **FAIL 错误码（典型）**：
  - `CELLBASE_MISSING`
  - `CELLBASE_FIRST_TRANSACTION_INVALID`
  - `MULTIPLE_CELLBASE_TRANSACTIONS`
  - `CELLBASE_OUTPUT_STRUCTURE_INVALID`
  - `CELLBASE_PREVIOUS_OUTPUT_NOT_NULL`
  - `CELLBASE_SINCE_MISMATCH`
  - `CELLBASE_INPUT_MISSING`
  - `CELLBASE_WITNESS_COUNT_INVALID`
  - `CELLBASE_WITNESS_INVALID`
  - `CELLBASE_TYPE_SCRIPT_PRESENT`
  - `CELLBASE_OUTPUT_DATA_NOT_EMPTY`

### 8.3 普通交易基础结构

下面这些检查只对 **非 cellbase** 交易适用；如果一个区块里没有非 cellbase 交易，这些字段会省略。

#### `check_transaction_version`

- **依赖**：`get_consensus.tx_version`
- **PASS 条件**：每笔非 cellbase 交易 `tx.version == consensus.tx_version`
- **FAIL 错误码**：`TRANSACTION_VERSION_MISMATCH`

#### `check_inputs_outputs_structure`

- **PASS 条件**：每笔非 cellbase 交易都必须同时满足：
  - `inputs` 非空
  - `outputs` 非空
- **FAIL 错误码**：`TRANSACTION_IO_EMPTY`

#### `check_outputs_data_length`

- **PASS 条件**：`outputs_data.len() == outputs.len()`
- **FAIL 错误码**：`OUTPUTS_DATA_LENGTH_MISMATCH`

#### `check_duplicate_cell_deps`

- **PASS 条件**：同一笔交易内，不允许出现重复的 `cell_dep(out_point, dep_type)`
- **FAIL 错误码**：`DUPLICATE_CELL_DEP`

#### `check_duplicate_header_deps`

- **PASS 条件**：同一笔交易内，不允许重复 `header_dep`
- **FAIL 错误码**：`DUPLICATE_HEADER_DEP`

#### `check_duplicate_inputs_in_transaction`

- **PASS 条件**：同一笔交易内，不允许重复消费同一个 input outpoint
- **FAIL 错误码**：`DUPLICATE_INPUT_IN_TRANSACTION`

#### `check_duplicate_inputs_in_block`

- **PASS 条件**：同一个区块内，不允许不同交易重复消费同一个 input outpoint
- **FAIL 错误码**：`DUPLICATE_INPUT_IN_BLOCK`

### 8.4 输入解析与索引

#### `check_input_content_resolution`

- **依赖**：`get_transaction(tx_hash, "0x2", true)`
- **PASS 条件**：每个输入引用的 source transaction 都必须同时满足：
  1. RPC 能返回交易
  2. `tx_status.status == Committed`
  3. `tx_status.block_hash` 存在且不是全零哈希
  4. `transaction` 字段里真的带回了完整 JSON 交易，而不是只给 hash
  5. 把这个 source 交易重新计算交易哈希后，必须等于被引用的 `tx_hash`
- **明确规则 FAIL**：
  - `INPUT_TX_HASH_MISMATCH`
- **会重试的缺数/暂不可验证类**：
  - `INPUT_TX_MISSING`
  - `INPUT_TX_NOT_COMMITTED`
  - `INPUT_TX_BLOCK_HASH_MISSING`
  - `INPUT_TX_JSON_MISSING`
  - `INPUT_SOURCE_BLOCK_HASH_ZERO`
- **请求异常类**：
  - `INPUT_TX_RPC_ERROR`

#### `check_input_output_index`

- **前提**：source transaction 已经解析成功
- **PASS 条件**：`input.previous_output.index < source_tx.outputs.len()`
- **FAIL 错误码**：`INPUT_OUTPUT_INDEX_OUT_OF_RANGE`
- **无法验证时**：
  - `INPUT_INDEX_UNVERIFIED`：通常是因为 source transaction 还拿不到，会跟 `check_input_content_resolution` 一起进入重试 / 执行 FAIL

### 8.5 输出 occupied capacity

#### `check_occupied_capacity`

- **依赖**：输出本身的 `lock` / `type` / `capacity` / `outputs_data`
- **实际比较口径**：
  1. 先取 `outputs_data[i].len()`
  2. 用 `OccupiedCapacity::bytes(len)` 得到 data bytes 占用
  3. 再用 `packed_output.occupied_capacity(data_capacity)` 计算**该完整 output 的实际最小 occupied capacity**
  4. 要求 `output.capacity >= occupied_capacity`
- **PASS 条件**：所有输出都不缺 occupied capacity
- **FAIL 错误码**：`OUTPUT_BELOW_OCCUPIED_CAPACITY`
- **诊断修正说明**：`expected_value` 现在记录的是**完整 output 的 occupied capacity**，不再误写成仅 data bytes 的容量

### 8.6 普通交易容量守恒

#### `check_ordinary_capacity_conservation`

- **适用条件**：
  - 非 cellbase 交易
  - 所有输入都能解析
  - 共识参数可用
  - 并且当前交易能被代码明确判断为“普通交易路径”，而不是 DAO 相关或分类不明
- **计算方式**：
  - `ordinary_input_sum = 所有普通输入 source output.capacity 之和`
  - `ordinary_output_sum = 当前交易所有 outputs.capacity 之和`
- **PASS 条件**：`ordinary_output_sum <= ordinary_input_sum`
- **FAIL 错误码**：
  - `OUTPUT_CAPACITY_EXCEEDS_INPUT`
  - `OUTPUT_CAPACITY_SUM_OVERFLOW`
- **无法完成时**：
  - `ORDINARY_CAPACITY_CLASSIFICATION_UNKNOWN`
  - `ORDINARY_CAPACITY_INCOMPLETE`
- **重试规则**：缺输入 / 分类暂不明时会按有限次数重试；耗尽后对外仍写 `FAIL`，但 `details.failure_kind = RETRY_EXHAUSTED`

### 8.7 Reward

reward 相关有两个字段，分别看金额总额与目标接收锁脚本。

#### `check_cellbase_reward_amount`

- **前提**：区块必须存在 cellbase
- **依赖**：
  - `get_consensus.tx_proposal_window.farthest`
  - `get_header_by_number(target_number)`
  - `get_block_economic_state(target_hash)`
  - 有时还需要 `get_block(target_hash)` 以解析目标块 cellbase witness lock
- **目标高度计算**：
  - `finalization_delay_length = farthest + 1`
  - 若 `block_number <= finalization_delay_length`，说明当前块还**没有最终确定的奖励目标块**
- **PASS 规则分两段**：
  1. **尚未到奖励可结算高度**：
     - 要求当前 cellbase `outputs` 为空
     - 为空则 PASS；非空则 FAIL（`REWARD_FINALIZATION_NOT_READY`）
  2. **已经有奖励目标块**：
     - `target_number = block_number - finalization_delay_length`
     - 取目标块经济状态，要求 `economic.finalized_at == audited_block.hash`
     - `expected = primary + secondary + committed + proposal`
     - 如果这个 `expected` 连放到目标 lock 下都达不到 occupied capacity，则当前块 cellbase 必须为空；否则 FAIL（`REWARD_BELOW_OCCUPIED_CAPACITY`）
     - 若可支付，则要求 `actual_cellbase_output_sum == expected`
- **明确规则 FAIL**：
  - `CELLBASE_MISSING`
  - `REWARD_FINALIZATION_NOT_READY`
  - `REWARD_BELOW_OCCUPIED_CAPACITY`
  - `CELLBASE_REWARD_MISMATCH`
- **暂不可验证 / 会进入执行 FAIL 的常见错误码**：
  - `REWARD_TARGET_HEADER_MISSING`
  - `REWARD_TARGET_HEADER_RPC_ERROR`
  - `REWARD_FINALIZATION_MISMATCH`
  - `REWARD_TARGET_BLOCK_MISMATCH`
  - `REWARD_TARGET_BLOCK_MISSING`
  - `REWARD_TARGET_BLOCK_RPC_ERROR`
  - `REWARD_TARGET_CELLBASE_MISSING`
  - `REWARD_TARGET_WITNESS_INVALID`
  - `ECONOMIC_STATE_MISSING`
  - `ECONOMIC_STATE_RPC_ERROR`

#### `check_cellbase_reward_target`

- **依赖**：和 `check_cellbase_reward_amount` 基本相同，但关注的是“付给谁”
- **PASS 条件**：
  1. 先定位目标块 `target_number`
  2. 取目标块 cellbase 的第一个 witness，解析出 `CellbaseWitness.lock`
  3. 在当前块 cellbase 的所有输出中，统计 `output.lock == expected_lock` 的容量和
  4. 要求 `paid_to_expected_lock == expected_reward`
- **明确规则 FAIL**：
  - `REWARD_FINALIZATION_NOT_READY`
  - `REWARD_BELOW_OCCUPIED_CAPACITY`
  - `CELLBASE_REWARD_TARGET_MISSING`
  - `CELLBASE_REWARD_TARGET_MISMATCH`
- **暂不可验证**：同上面 reward amount 的缺数据 / RPC 失败类

### 8.8 DAO 提现容量上限

#### `check_dao_withdraw_capacity`

- **适用条件**：交易里至少有一个输入被当前代码识别为 DAO 相关输入
- **DAO 输入识别方式**：source output 存在 `type`，且：
  - `hash_type == Type`
  - `code_hash == consensus.dao_type_hash`
- **依赖**：
  - source transaction 本身
  - source output data
  - 有时依赖当前交易最后一个 `header_dep`
  - `calculate_dao_maximum_withdraw(...)`
- **代码里的分支逻辑**：
  1. **source output data 全 0**：
     - 当作 deposit cell 的直接引用
     - 必须拿当前交易最后一个 `header_dep` 作为 `WithdrawingHeaderHash(...)`
     - 若没有这个 header，报 `DAO_WITHDRAW_HEADER_MISSING`
  2. **source output data 非全 0**：
     - 代码尝试追溯 deposit out point：
       - 若 source tx 只有 1 个 input，用 `source_tx.inputs[0].previous_output`
       - 否则若 `source_output_index < source_tx.inputs.len()`，用同 index 的 input.previous_output
       - 否则报 `DAO_DEPOSIT_REFERENCE_AMBIGUOUS`
     - 之后调用 `calculate_dao_maximum_withdraw(deposit_out_point, WithdrawingOutPoint(current_input.previous_output))`
- **汇总公式**：
  - `expected = ordinary_input_sum + dao_effective_sum`
  - `ordinary_output_sum <= expected` 才 PASS
- **明确规则 FAIL**：
  - `DAO_WITHDRAW_CAPACITY_EXCEEDED`
  - `INPUT_TX_OUTPUT_DATA_MISSING`
- **暂不可验证 / 执行失败**：
  - `DAO_WITHDRAW_HEADER_MISSING`
  - `DAO_DEPOSIT_REFERENCE_AMBIGUOUS`
  - `DAO_MAXIMUM_WITHDRAW_MISSING`
  - `DAO_MAXIMUM_WITHDRAW_RPC_ERROR`
  - `DAO_WITHDRAW_CAPACITY_INCOMPLETE`
  - `DAO_CLASSIFICATION_UNKNOWN`

其中：

- `DAO_MAXIMUM_WITHDRAW_MISSING` 之类“看起来应当有数据却返回 null”的情况，会做有限重试
- `DAO_DEPOSIT_REFERENCE_AMBIGUOUS` 这种当前实现本身无法无歧义追踪的情况，不会靠无限重试掩盖，而会在最终日志里以执行 FAIL 保留下来

---

## 9. 当前已知实现限制（请务必结合日志语义理解）

- 仍然**不做** PoW / VM / cycles / since / maturity / full liveness 审计
- `check_output_lock_hash_type` 当前没有对外输出 PASS，README 也不把它写进“已通过判据”
- reward / DAO 依赖同一节点 RPC 的一致性；如果节点对这些 RPC 的历史数据返回不完整、不一致，本工具会优先把它记为执行 FAIL，而不是伪造 PASS
- `canonical_at_audit=true` 只表示本次尝试开始时该高度当前规范块仍是它，不代表整个审计过程内链绝对没有变化
- 无 cursor 模式不会落盘待补审状态；同一进程内可补审，重启后不能恢复停机期间或上次失败但未完成的待补审高度

---

## 10. 构建与测试

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
cargo build --release --locked
```

当前回归测试重点覆盖：

- schema v4：删除 `coverage` / `unknown_checks`、只输出 PASS/FAIL
- `failed_checks` 在 `details` 截断、甚至 `max_details=0` 时仍完整
- 明确规则 FAIL 与执行 FAIL 的区分
- source transaction 暂时返回 `null` 后恢复 PASS
- source transaction 持续缺失时重试耗尽、输出执行 FAIL，并携带 `failure_kind` / `rpc_method` / `attempts` / `max_retries`
- 执行失败区块不推进完成进度：
  - 无 cursor 模式下，同一进程内 tip 前移也会先补审失败块
  - 持久化 cursor 模式下，重启后仍会继续补审失败块
- 补审恢复后，同一高度会追加新的 PASS 日志，而不是覆盖旧 FAIL 日志
- genesis 相关的 parent-hash 适用性处理
- reward 目标块 lock 校验
- 普通容量失败与输入缺失场景回归

