# ckb-block-auditor

CKB Block Auditor（当前 crate 版本 `0.2.0`）。

通过 **CKB JSON-RPC** 审计当前连接节点看到的规范链区块，输出单行 JSON 日志，方便接入日志采集与告警。

`PASS` 表示：该次审计里**所有适用且已执行的检查项**都通过。

---

## 1. 运行方式

### 实时模式（不落盘 cursor）

启动时先审计当前 tip；之后只跟随新块。

> 不会创建默认 cursor 文件；进程重启后不会自动补审停机期间错过的区块。

```bash
cargo run -- \
  --rpc-url http://127.0.0.1:8114 \
  --node-id ckb-node-01 \
  --log-path ./audit.log \
  --poll-interval-ms 3000
```

### 持久化模式（显式指定 cursor）

- 文件不存在：从当前 tip 开始审计并创建 cursor
- 文件存在：从已保存进度继续，补审停机期间漏掉的区块

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

## 2. CLI / 环境变量

| 参数 | 环境变量 | 默认值 | 作用 |
|---|---|---:|---|
| `--rpc-url` | `CKB_RPC_URL` | `http://127.0.0.1:8114` | CKB JSON-RPC 地址 |
| `--node-id` | `CKB_NODE_ID` | `unknown` | 写入日志 `node_id` |
| `--poll-interval-ms` | `CKB_POLL_INTERVAL_MS` | `3000` | 轮询间隔 |
| `--rpc-timeout-secs` | `CKB_RPC_TIMEOUT_SECS` | `10` | 单次 HTTP RPC 请求超时 |
| `--max-retries` | `CKB_MAX_RETRIES` | `2` | 额外重试次数（`2` 表示最多 3 次总尝试） |
| `--cursor-path` | `CKB_CURSOR_PATH` | 无 | 启用持久化 cursor |
| `--log-path` | `CKB_LOG_PATH` | 无 | 输出文件；不填则 stdout |
| `--max-details` | `CKB_MAX_DETAILS` | `200` | 每条日志最多保留多少条 `details` |
| `--max-future-ms` | `CKB_MAX_FUTURE_MS` | `15000` | 时间戳检查允许的未来偏移上限 |
| `--median-time-span` | `CKB_MEDIAN_TIME_SPAN` | `11` | 解析参数，但时间戳检查实际使用 `get_consensus.median_time_block_count` |
| `--proposal-limit` | `CKB_PROPOSAL_LIMIT` | `1500` | 解析参数，但 proposal 上限实际使用 `get_consensus.max_block_proposals_limit` |
| `--tx-version` | `CKB_TX_VERSION` | `0` | 解析参数，但交易版本实际使用 `get_consensus.tx_version` |
| `--history-retention` | `CKB_HISTORY_RETENTION` | `256` | cursor 历史哈希保留深度 |
| `--dao-type-hash` | `CKB_DAO_TYPE_HASH` | 空字符串 | 若配置，要求与 `get_consensus.dao_type_hash` 一致 |

固定退避策略：100ms、200ms、400ms...（指数退避，最大 64 倍基础间隔）。

---

## 3. 进度、重试、重组

### 无 `--cursor-path`

- 不读写任何 cursor 文件
- 同一进程内，执行失败的高度会优先补审，不会被 tip 前移直接跳过
- 进程重启后不会恢复内存里的待补审状态

### 有 `--cursor-path`

- cursor 绑定 `genesis_hash`
- genesis 不一致会报错，且不会覆盖原文件
- 同链更换 RPC（genesis 一致）可继续使用同一 cursor

### 规则 FAIL vs 执行 FAIL

- **规则 FAIL**：已拿到足够数据并判定规则不通过；该高度视为已完成，可推进已完成游标
- **执行 FAIL**：必需数据缺失 / RPC 失败 / 歧义无法解析等导致检查未完成；该高度不视为已完成，后续会继续补审

### `canonical_at_audit`

`canonical_at_audit=true` 仅表示本次审计开始时该高度的当前规范块是这条日志里的 `block_hash`。

---

## 4. 日志与 schema v4

- `result` 仅有 `PASS` / `FAIL`
- 完成一次审计尝试后的最终日志里，`check_*` 输出仅有 `PASS` / `FAIL`（`NotApplicable` 不输出）
- 删除 `coverage` / `unknown_checks`
- `failed_checks` 汇总所有最终输出为 FAIL 的检查项
- 即使 `details` 被截断或 `max_details=0`，`failed_checks` 仍完整

`details` 常见字段：

- `failure_kind`：`VALIDATION_FAILED` / `RETRY_EXHAUSTED` / `EXECUTION_FAILED`
- `rpc_method`、`attempts`、`max_retries`
- 以及 `tx_hash`、`tx_index`、`input_index`、`output_index`、`expected_*`、`actual_value` 等上下文

检查项数量不是固定值，会随区块内容变化（例如是否存在非 cellbase 交易、是否存在可识别 DAO 输入）。

---

## 5. 已实现检查（按 check_name）

下列字段会按适用条件输出在最终日志中。

### 5.1 头部与块体基础

- `check_block_height`：父块存在时要求 `number == parent.number + 1`（`BLOCK_NUMBER_MISMATCH`）
- `check_parent_hash`：重算父块哈希后要求等于当前 `parent_hash`（`PARENT_HASH_MISMATCH`）
- `check_epoch_continuity`：父子 epoch 连续性（`EPOCH_CONTINUITY_FAIL`）
- `check_timestamp`：
  - 数据来源：父块及向前追溯的祖先头、`get_consensus.median_time_block_count`
  - PASS：`timestamp > median(ancestor_timestamps)` 且 `timestamp <= now + max_future_ms`
  - 规则 FAIL：`TIMESTAMP_OUT_OF_RANGE`
  - 执行失败常见：`TIMESTAMP_ANCESTOR_UNAVAILABLE`、`TIMESTAMP_MEDIAN_INCOMPLETE`
- `check_block_version`：
  - 数据来源：块头 `version` 与 `get_consensus.block_version`
  - PASS：两者相等
  - FAIL：`BLOCK_VERSION_MISMATCH`
- `check_block_size`：块序列化大小（不含 uncle proposals）`<= get_consensus.max_block_bytes`（`BLOCK_SIZE_EXCEEDED`）
- `check_uncle_count_limit`：`uncles.len() <= get_consensus.max_uncles_num`（`UNCLE_COUNT_EXCEEDED`）
- `check_proposal_limit`：`proposals.len() <= get_consensus.max_block_proposals_limit`（`PROPOSAL_LIMIT_EXCEEDED`）

### 5.2 哈希与重复项

- `check_block_hash`：重算块头哈希一致（`BLOCK_HASH_MISMATCH`）
- `check_transaction_hashes`：逐笔交易哈希一致（`TRANSACTION_HASH_MISMATCH`）
- `check_transactions_root`：重算 `transactions_root` 一致（`TRANSACTIONS_ROOT_MISMATCH`）
- `check_proposals_hash`：重算 `proposals_hash` 一致（`PROPOSALS_HASH_MISMATCH`）
- `check_extra_hash`：重算 `extra_hash` 一致（`EXTRA_HASH_MISMATCH`）
- `check_duplicate_transactions`：区块内交易哈希无重复（`DUPLICATE_TRANSACTION`）
- `check_duplicate_proposals`：区块内 proposal 短 ID 无重复（`DUPLICATE_PROPOSAL`）

### 5.3 交易与输入输出（仅对非 cellbase 交易适用）

- `check_cellbase_structure`：cellbase 位置/输入结构
- `check_transaction_version`：`tx.version == get_consensus.tx_version`（`TRANSACTION_VERSION_MISMATCH`）
- `check_inputs_outputs_structure`：非 cellbase 交易必须有 input 且有 output（`TRANSACTION_IO_EMPTY`）
- `check_outputs_data_length`：`outputs.len == outputs_data.len`（`OUTPUTS_DATA_LENGTH_MISMATCH`）
- `check_output_lock_hash_type`：
  - 数据来源：区块内交易输出 lock script `hash_type` 编码
  - PASS：`hash_type` 编码满足 CKB 共识允许值（`0x01` 或任意偶数字节 `0x00..0xfe`）
  - FAIL：`OUTPUT_LOCK_HASH_TYPE_INVALID`
  - 另外：若 RPC 响应本身含非法 `hash_type`，会在 `get_block*` 解码阶段失败并作为执行失败处理
- `check_duplicate_cell_deps`（`DUPLICATE_CELL_DEP`）
- `check_duplicate_header_deps`（`DUPLICATE_HEADER_DEP`）
- `check_duplicate_inputs_in_transaction`（`DUPLICATE_INPUT_IN_TRANSACTION`）
- `check_duplicate_inputs_in_block`（`DUPLICATE_INPUT_IN_BLOCK`）
- `check_input_content_resolution`：通过 `get_transaction` 解析引用输入（缺失/RPC 错误会进入执行失败）
- `check_input_output_index`：输入引用 output 索引必须有效（`INPUT_OUTPUT_INDEX_OUT_OF_RANGE`）
- `check_occupied_capacity`：output 容量满足 occupied capacity（`OUTPUT_BELOW_OCCUPIED_CAPACITY`）
- `check_ordinary_capacity_conservation`：普通交易输出总容量不超过普通输入总容量（`OUTPUT_CAPACITY_EXCEEDS_INPUT`）

### 5.4 Cellbase reward

#### `check_cellbase_reward_amount`

- 依赖 RPC：`get_consensus`、`get_header_by_number`、`get_block_economic_state`、`get_block`
- 计算：
  - 根据 `finalization_delay_length = tx_proposal_window.farthest + 1` 定位目标块
  - 要求 `economic.finalized_at == audited_block.hash`
  - `expected_reward = primary + secondary + committed + proposal`
  - 若奖励放入目标 lock 后仍低于 occupied capacity，则当前块 cellbase 应为空
- 规则 FAIL：`CELLBASE_MISSING`、`REWARD_FINALIZATION_NOT_READY`、`REWARD_BELOW_OCCUPIED_CAPACITY`、`CELLBASE_REWARD_MISMATCH`
- 执行失败常见：`REWARD_TARGET_HEADER_MISSING`、`REWARD_TARGET_HEADER_RPC_ERROR`、`REWARD_TARGET_BLOCK_MISSING`、`REWARD_TARGET_BLOCK_RPC_ERROR`、`ECONOMIC_STATE_MISSING`、`ECONOMIC_STATE_RPC_ERROR`

#### `check_cellbase_reward_target`

- 依赖 RPC：同上
- 计算：解析目标块 cellbase witness 中的 `lock`，统计当前块 cellbase 输出里支付到该 lock 的容量，要求等于 `expected_reward`
- 规则 FAIL：`REWARD_FINALIZATION_NOT_READY`、`REWARD_BELOW_OCCUPIED_CAPACITY`、`CELLBASE_REWARD_TARGET_MISSING`、`CELLBASE_REWARD_TARGET_MISMATCH`
- 执行失败常见：与 reward amount 类似（缺数据/RPC/解析失败路径）

### 5.5 DAO 提现容量上限

#### `check_dao_withdraw_capacity`

- 适用：交易中存在被识别为 DAO 的输入
- DAO 输入识别：source output.type 存在，且 `hash_type == Type` 且 `code_hash == get_consensus.dao_type_hash`
- 依赖 RPC：source 交易、source output data、必要时 `header_dep`、`calculate_dao_maximum_withdraw`
- 判据：`ordinary_output_sum <= ordinary_input_sum + dao_effective_sum`
- 规则 FAIL：`DAO_WITHDRAW_CAPACITY_EXCEEDED`、`INPUT_TX_OUTPUT_DATA_MISSING`
- 执行失败常见：`DAO_WITHDRAW_HEADER_MISSING`、`DAO_DEPOSIT_REFERENCE_AMBIGUOUS`、`DAO_MAXIMUM_WITHDRAW_MISSING`、`DAO_MAXIMUM_WITHDRAW_RPC_ERROR`、`DAO_WITHDRAW_CAPACITY_INCOMPLETE`、`DAO_CLASSIFICATION_UNKNOWN`

---

## 6. 构建与测试

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
cargo build --release --locked
```
