# ckb-block-auditor

CKB Block Auditor（当前 crate 版本 `0.2.0`）是一个**只读**的 CKB JSON-RPC 区块审计工具：它从节点读取规范链区块与相关上下文，针对当前版本**已经实现**的规则做校验，并输出单行 JSON，方便接入日志采集、告警或离线复核。

`PASS` 只表示：该区块在**本工具当前已实现且适用**的检查项上全部通过，并且这些检查都拿到了最终结论。

---

## 1. 项目简介

- 面向对象：提供 CKB JSON-RPC 的节点或网关。
- 工作方式：启动时先审计当前 tip；之后轮询并跟随新块。
- 输出位置：最终审计结果输出到 stdout，或写入 `--log-path` 指定文件；运行中的冷却、重试、pending 诊断始终写到 stderr。
- 适用场景：节点接入前抽查、线上巡检、区块异常告警、为后续分析保留结构化审计日志。

---

## 2. 快速开始 / 运行方式

### 2.1 前置条件

1. 已安装 Rust 工具链（需要 `cargo`）。
2. 能访问一个 CKB JSON-RPC 端点。
3. 若要持久化进度或文件日志，运行目录对相应路径有写权限。

### 2.2 实时模式（不落盘 cursor）

不传 `--cursor-path` 时：启动只从**当前 tip**开始；同一进程内会继续补审暂未完成的高度，但进程重启后不会自动补审停机期间错过的区块。

```bash
cargo run -- \
  --rpc-url http://127.0.0.1:8114 \
  --node-id ckb-node-01 \
  --log-path ./audit.log \
  --poll-interval-ms 3000
```

- 最终区块审计 JSON：写入 `./audit.log`
- 运行诊断（如 429 冷却、pending 恢复）：写入 stderr

### 2.3 持久化模式（显式指定 cursor）

传入 `--cursor-path` 后：

- 文件不存在：从当前 tip 开始审计，并创建 cursor
- 文件存在：按保存进度继续，补审停机期间漏掉的区块
- cursor 与所连链的 `genesis_hash` 绑定；切换到不同链不会静默复用旧进度

```bash
cargo run -- \
  --rpc-url https://mainnet.ckbapp.dev \
  --node-id ckb-mainnet-01 \
  --cursor-path ./data/mainnet.cursor.json \
  --log-path ./audit-mainnet.log \
  --poll-interval-ms 3000
```

### 2.4 release 二进制

```bash
cargo build --release --locked
./target/release/ckb-block-auditor \
  --rpc-url https://mainnet.ckbapp.dev \
  --node-id ckb-mainnet-01 \
  --cursor-path ./data/mainnet.cursor.json \
  --log-path ./audit-mainnet.log
```

> 429、缺数据或其他暂时无法完成的情况不会先写一条区块 `FAIL`；该高度会保持 pending，等拿到最终结论后才输出一条最终 JSON。

---

## 3. 配置说明

| 参数 | 环境变量 | 默认值 | 单位 | 当前含义 |
|---|---|---:|---|---|
| `--rpc-url` | `CKB_RPC_URL` | `http://127.0.0.1:8114` | - | CKB JSON-RPC 地址 |
| `--node-id` | `CKB_NODE_ID` | `unknown` | - | 原样写入日志 `node_id` |
| `--poll-interval-ms` | `CKB_POLL_INTERVAL_MS` | `3000` | ms | 每轮 `poll_once` 之间的等待时间 |
| `--rpc-timeout-secs` | `CKB_RPC_TIMEOUT_SECS` | `10` | s | 单次 HTTP RPC 请求超时 |
| `--max-retries` | `CKB_MAX_RETRIES` | `2` | 次 | 每个 HTTP 请求、每个区块额外重试次数；总尝试次数 = `max_retries + 1` |
| `--rpc-min-interval-ms` | `CKB_RPC_MIN_INTERVAL_MS` | `100` | ms | 全局请求最小间隔（所有方法/重试/并发共享），用于平滑主动限速 |
| `--rpc-max-interval-ms` | `CKB_RPC_MAX_INTERVAL_MS` | `2000` | ms | 自适应降速上限；遇到 429 会在该上限内放慢，持续成功后逐步恢复 |
| `--rpc-max-concurrency` | `CKB_RPC_MAX_CONCURRENCY` | `2` | 请求数 | 全局并发上限（所有方法共享），避免恢复时瞬时并发突发 |
| `--cursor-path` | `CKB_CURSOR_PATH` | 无 | 路径 | 启用持久化 cursor；不传则只保留进程内状态 |
| `--log-path` | `CKB_LOG_PATH` | 无 | 路径 | 最终审计 JSON 输出文件；不传则输出到 stdout |
| `--max-details` | `CKB_MAX_DETAILS` | `200` | 条 | 每条最终日志最多保留多少条 `details`；`failed_checks` 仍保持完整 |
| `--max-future-ms` | `CKB_MAX_FUTURE_MS` | `15000` | ms | `check_timestamp` 允许区块时间领先审计机时钟的最大偏移 |
| `--history-retention` | `CKB_HISTORY_RETENTION` | `256` | 高度/条 | cursor 历史哈希保留深度，也用于文件日志的近期重复输出恢复窗口 |
| `--header-cache-capacity` | `CKB_HEADER_CACHE_CAPACITY` | `8192` | 条 | 只读内存头部缓存上限（按 hash 键控，满后淘汰） |
| `--block-cache-entries` | `CKB_BLOCK_CACHE_ENTRIES` | `64` | 条 | 只读内存块缓存条目上限（按 hash 键控） |
| `--block-cache-max-bytes` | `CKB_BLOCK_CACHE_MAX_BYTES` | `67108864` | bytes | 只读内存块缓存总字节上限；超限按旧条目淘汰 |
| `--stats-interval-secs` | `CKB_STATS_INTERVAL_SECS` | `60` | s | stderr 运行统计输出间隔（HTTP 尝试/429/缓存命中/进度） |
| `--dao-type-hash` | `CKB_DAO_TYPE_HASH` | 空字符串 | hash | 可选启动保护：若配置，必须与 `get_consensus.dao_type_hash` 一致 |
| `--median-time-span` | `CKB_MEDIAN_TIME_SPAN` | `11` | - | 当前仅解析参数；已实现时间戳检查实际使用 `get_consensus.median_time_block_count` |
| `--proposal-limit` | `CKB_PROPOSAL_LIMIT` | `1500` | - | 当前仅解析参数；已实现提案上限检查实际使用 `get_consensus.max_block_proposals_limit` |
| `--tx-version` | `CKB_TX_VERSION` | `0` | - | 当前仅解析参数；已实现交易版本检查实际使用 `get_consensus.tx_version` |

---

## 4. 输出示例

最终结果每个区块只输出**一条** JSON 行，例如：

```json
{"timestamp":"2026-09-14T08:31:25.000Z","schema_version":4,"service":"ckb-block-auditor","auditor_version":"0.2.0","node_id":"ckb-mainnet-01","block_height":20451139,"block_hash":"0x47f6b1825527359db4a2a1b316e70c3eb419f3200df191bcd46c60866cc8a38e","parent_hash":"0xdafc14c4264dcafcf2f66571d3b9b7b31ea3a2aae9033470b2f2b421375d5e16","block_timestamp":1789374287565,"canonical_at_audit":true,"result":"PASS","audit_duration_ms":2512,"check_block_height":"PASS","check_parent_hash":"PASS","check_epoch_continuity":"PASS","check_timestamp":"PASS","check_block_size":"PASS","check_proposal_limit":"PASS","check_block_hash":"PASS","check_transaction_hashes":"PASS","check_transactions_root":"PASS","check_proposals_hash":"PASS","check_extra_hash":"PASS","check_duplicate_transactions":"PASS","check_duplicate_proposals":"PASS","check_cellbase_structure":"PASS","check_cellbase_reward_amount":"PASS","check_cellbase_reward_target":"PASS"}
```

阅读要点：

- `result`：最终只有 `PASS` / `FAIL`
- `check_*`：只输出本块**适用**的检查；`NotApplicable` 不写入 JSON
- `failed_checks`：出现失败时汇总所有失败检查名
- `details`：出现失败时给出代表性的 `error_code`、交易位置、期望值/实际值等上下文
- `canonical_at_audit=true`：表示开始审计该高度时，当前规范块就是这条日志里的 `block_hash`

---

## 5. 已实现检查

下列内容按当前 `src/lib.rs` 的实际实现整理；只描述已经真正参与运行结果的检查。

### 5.1 头部与块体基础

#### `check_block_height`

- 适用：除 genesis 外的所有区块。
- 数据来源：当前块头、`get_header(parent_hash)`。
- 判据：`current.number == parent.number + 1`，单位是区块高度。
- 代表性失败：`BLOCK_NUMBER_MISMATCH`。

#### `check_parent_hash`

- 适用：除 genesis 外的所有区块。
- 数据来源：父块头。
- 判据：先把父块头重新编码并计算 header hash，再与当前块 `parent_hash` 比较；两者必须完全相等。
- 代表性失败：`PARENT_HASH_MISMATCH`。

#### `check_epoch_continuity`

- 适用：除 genesis 外的所有区块。
- 数据来源：父块 `epoch` 与当前块 `epoch`。
- 判据：
  - 当前 `epoch` 编码必须是良构值；
  - 若父块 epoch 视为 genesis，则通过；
  - 否则当前 epoch 必须是父 epoch 的合法后继（代码使用 `is_successor_of`）。
- 代表性失败：`EPOCH_CONTINUITY_FAIL`，reason 会带出 `number:index:length` 三元组，方便定位是格式问题还是父子衔接问题。

#### `check_timestamp`

- 适用：除 genesis 外的所有区块。
- 数据来源：父块开始向前追溯的祖先头、`get_consensus.median_time_block_count`、审计机当前时钟、`--max-future-ms`。
- 采样规则：
  - 样本从**父块时间戳**开始；
  - 最多取 `median_time_block_count` 个样本；
  - 若向前追溯到零哈希（到达链头附近），会用已经拿到的样本继续计算中位数；
  - 若中途因为 RPC/缺头拿不全样本，则这一轮无法得出最终区块结果。
- PASS 条件：`block_timestamp > median(ancestor_timestamps)` 且 `block_timestamp <= 审计机当前时间 + max_future_ms`，单位都是毫秒。
- 代表性失败：`TIMESTAMP_OUT_OF_RANGE`；常见未完成错误有 `TIMESTAMP_ANCESTOR_UNAVAILABLE`、`TIMESTAMP_MEDIAN_INCOMPLETE`。

#### `check_block_version`

- 适用：所有区块。
- 数据来源：块头 `version`、`get_consensus.block_version`。
- PASS 条件：二者完全相等。
- 代表性失败：`BLOCK_VERSION_MISMATCH`。

#### `check_block_size`

- 适用：所有区块。
- 数据来源：当前块序列化结果、`get_consensus.max_block_bytes`。
- PASS 条件：`serialized_size_without_uncle_proposals() <= max_block_bytes`，单位字节。
- 边界说明：这里比的是**不含 uncle proposals** 的块体序列化大小，文档应以实现为准。
- 代表性失败：`BLOCK_SIZE_EXCEEDED`。

#### `check_uncle_count_limit`

- 适用：所有区块。
- 数据来源：`block.uncles.len()`、`get_consensus.max_uncles_num`。
- PASS 条件：`uncles.len() <= max_uncles_num`。
- 代表性失败：`UNCLE_COUNT_EXCEEDED`。

#### `check_proposal_limit`

- 适用：所有区块。
- 数据来源：`block.proposals.len()`、`get_consensus.max_block_proposals_limit`。
- PASS 条件：`proposals.len() <= max_block_proposals_limit`。
- 代表性失败：`PROPOSAL_LIMIT_EXCEEDED`。

### 5.2 哈希重算与区块内重复项

#### `check_block_hash`

- 适用：所有区块。
- 数据来源：当前块头本身。
- PASS 条件：重新计算 header hash 后，必须等于 RPC 返回的 `block.hash`。
- 代表性失败：`BLOCK_HASH_MISMATCH`。

#### `check_transaction_hashes`

- 适用：所有区块。
- 数据来源：区块内每一笔交易。
- PASS 条件：对每笔交易分别重算 tx hash，并逐笔与 RPC 返回的 `tx.hash` 比较。
- 代表性失败：`TRANSACTION_HASH_MISMATCH`；失败详情会带 `tx_index` 和该笔交易哈希。

#### `check_transactions_root`

- 适用：所有区块。
- 数据来源：完整块体与块头字段 `transactions_root`。
- PASS 条件：重算 `calc_transactions_root()` 后，与头部 `transactions_root` 完全相等。
- 代表性失败：`TRANSACTIONS_ROOT_MISMATCH`。

#### `check_proposals_hash`

- 适用：所有区块。
- 数据来源：块内 proposal 列表与头部 `proposals_hash`。
- PASS 条件：重算 `calc_proposals_hash()` 后完全相等。
- 代表性失败：`PROPOSALS_HASH_MISMATCH`。

#### `check_extra_hash`

- 适用：所有区块。
- 数据来源：完整块体与头部 `extra_hash`。
- PASS 条件：重算 `calc_extra_hash().extra_hash()` 后完全相等。
- 代表性失败：`EXTRA_HASH_MISMATCH`。

#### `check_duplicate_transactions`

- 适用：所有区块。
- 重复键：交易哈希字符串 `0x...`。
- PASS 条件：同一块内每个交易哈希只出现一次。
- 代表性失败：`DUPLICATE_TRANSACTION`。

#### `check_duplicate_proposals`

- 适用：所有区块。
- 重复键：proposal short id 的原始字节序列。
- PASS 条件：同一块内每个 proposal short id 只出现一次。
- 代表性失败：`DUPLICATE_PROPOSAL`。

### 5.3 Cellbase 结构

#### `check_cellbase_structure`

- 适用：所有区块。
- 数据来源：区块第一笔交易、其输入/输出/witness。
- 当前实现同时覆盖以下约束：
  1. 区块必须有第 0 笔交易，否则 `CELLBASE_MISSING`。
  2. 第 0 笔必须能被 `is_cellbase()` 识别为 cellbase，否则 `CELLBASE_FIRST_TRANSACTION_INVALID`。
  3. 除第 0 笔外，后续交易都不能再是 cellbase，否则 `MULTIPLE_CELLBASE_TRANSACTIONS`。
  4. cellbase `outputs.len() <= 1`、`outputs_data.len() <= 1`，且两者长度必须相等；因此它允许**零输出**或**一个输出**，不允许更多输出。
  5. 必须存在第一个输入；该输入 `previous_output` 必须是 null out point，且 `since == block_number`。
  6. `witnesses.len() == 1`，并且这个 witness 必须能解码成 `CellbaseWitness`。
  7. 如果存在唯一输出：`type_` 必须为空，`outputs_data[0]` 必须是空字节串。
- 代表性失败：`CELLBASE_OUTPUT_STRUCTURE_INVALID`、`CELLBASE_PREVIOUS_OUTPUT_NOT_NULL`、`CELLBASE_SINCE_MISMATCH`、`CELLBASE_WITNESS_INVALID`、`CELLBASE_TYPE_SCRIPT_PRESENT`、`CELLBASE_OUTPUT_DATA_NOT_EMPTY`。

### 5.4 非 cellbase 交易：版本、依赖、输入输出与容量

以下检查只对**非 cellbase 交易**适用；如果一个区块只有 cellbase，这些字段不会出现在最终 JSON 中。

#### `check_transaction_version`

- 数据来源：每笔非 cellbase 交易 `tx.version`、`get_consensus.tx_version`。
- PASS 条件：每笔交易的版本都必须与共识版本完全相等。
- 代表性失败：`TRANSACTION_VERSION_MISMATCH`。

#### `check_inputs_outputs_structure`

- PASS 条件：每笔非 cellbase 交易都必须同时满足 `inputs.len() > 0` 且 `outputs.len() > 0`。
- 代表性失败：`TRANSACTION_IO_EMPTY`。

#### `check_outputs_data_length`

- PASS 条件：每笔非 cellbase 交易都必须满足 `outputs.len() == outputs_data.len()`。
- 代表性失败：`OUTPUTS_DATA_LENGTH_MISMATCH`。

#### `check_output_lock_hash_type`

- 数据来源：每个输出 lock script 的**底层序列化 `hash_type` 字节**。
- PASS 条件：该字节必须满足 `ScriptHashType::verify_value`；按当前代码与测试，接受值是 `0x01`，或任意偶数字节 `0x00..0xfe`。
- 适用范围：只检查**非 cellbase 交易输出**的 lock script；不根据 JSON 字符串名猜测，也不执行脚本。
- 代表性失败：`OUTPUT_LOCK_HASH_TYPE_INVALID`。

#### `check_duplicate_cell_deps`

- 重复键：`{tx_hash}:{index}:{dep_type}`。
- PASS 条件：同一笔交易内每个 `cell_dep` 只能出现一次。
- 代表性失败：`DUPLICATE_CELL_DEP`。

#### `check_duplicate_header_deps`

- 重复键：header hash。
- PASS 条件：同一笔交易内每个 `header_dep` 只能出现一次。
- 代表性失败：`DUPLICATE_HEADER_DEP`。

#### `check_duplicate_inputs_in_transaction`

- 重复键：输入引用的 out point，即 `{tx_hash}:{index}`。
- PASS 条件：同一笔交易内不能重复消费同一个 out point。
- 代表性失败：`DUPLICATE_INPUT_IN_TRANSACTION`。

#### `check_duplicate_inputs_in_block`

- 重复键：同样是 `{tx_hash}:{index}`，但范围扩大到**同一块的全部非 cellbase 交易**。
- PASS 条件：一个 out point 不能被同块内两笔不同交易同时消费。
- 代表性失败：`DUPLICATE_INPUT_IN_BLOCK`。

#### `check_input_content_resolution`

- 数据来源：对每个输入调用 `get_transaction(previous_output.tx_hash)`。
- PASS 条件：源交易必须满足：
  1. `tx_status.status == Committed`
  2. `tx_status.block_hash` 存在
  3. RPC 返回了完整 JSON 交易体
  4. 用返回交易重算出的 tx hash 必须等于被请求的 hash
- 这个检查验证的是“引用内容能否被可靠解析并自洽”，不是历史 live-cell 校验。
- 代表性失败或未完成：`INPUT_TX_NOT_COMMITTED`、`INPUT_TX_BLOCK_HASH_MISSING`、`INPUT_TX_JSON_MISSING`、`INPUT_TX_HASH_MISMATCH`、`INPUT_TX_MISSING`、`INPUT_TX_RPC_ERROR`。

#### `check_input_output_index`

- 数据来源：已经解析出的源交易输出列表。
- PASS 条件：`previous_output.index < source_tx.outputs.len()`。
- 代表性失败：`INPUT_OUTPUT_INDEX_OUT_OF_RANGE`。
- 若源交易本身还未成功解析，则这一项也会随之保持未完成，例如 `INPUT_INDEX_UNVERIFIED`。

#### `check_occupied_capacity`

- 数据来源：当前交易每个输出、同位置 `outputs_data`。
- PASS 条件：`output.capacity >= occupied_capacity(output, outputs_data)`，单位 shannon。
- 计算方式：代码先把 `outputs_data[i]` 的字节数换算成 data capacity，再用 `CellOutput::occupied_capacity(...)` 计算该输出的最小占用容量。
- 代表性失败：`OUTPUT_BELOW_OCCUPIED_CAPACITY`。

#### `check_ordinary_capacity_conservation`

- 适用：能被当前实现明确判定为**不含 DAO 输入**的非 cellbase 交易。
- 普通输入容量来源：把每个已成功解析、且**不是 DAO type output** 的源输出容量累加。
- 普通输出容量来源：当前交易所有输出容量求和，单位 shannon。
- PASS 条件：`ordinary_output_sum <= ordinary_input_sum`。
- 边界说明：
  - 如果任一输入无法解析、源块哈希为全零，或当前实现无法确定输入是否属于 DAO，区块会保留 pending，等待更多数据后再给最终结论。
  - 这里的“ordinary_output_sum”按实现是**全部输出容量之和**，不会再按输出脚本二次分类。
- 代表性失败：`OUTPUT_CAPACITY_EXCEEDS_INPUT`。

### 5.5 Cellbase reward

这两项都依赖 `get_consensus`、`get_header_by_number`、`get_block_economic_state`、`get_block`，并且都围绕“当前块是否正好领取了某个目标块已经 finalized 的矿工奖励”。

#### `check_cellbase_reward_amount`

- 目标高度：`target_height = block_height - (tx_proposal_window.farthest + 1)`；实现里把这个值缓存成 `finalization_delay_length`。
- 早期区块边界：当 `block_height <= finalization_delay_length` 时，当前块还没有 finalized reward target；此时 **cellbase 必须没有输出**，否则失败码是 `REWARD_FINALIZATION_NOT_READY`。
- 奖励金额来源：`get_block_economic_state(target_hash).miner_reward` 的四项之和：
  `primary + secondary + committed + proposal`，单位 shannon。
- 额外一致性条件：`economic.finalized_at` 必须等于**当前被审计块**的 hash；否则这一轮无法确认最终奖励归属。
- PASS 条件：
  1. 若奖励金额连“以目标 lock 建一个 cellbase 输出”的 occupied capacity 都不够，则当前块 cellbase 应为空；
  2. 否则，当前块 cellbase **全部输出容量总和**必须恰好等于 `expected_reward`。
- 代表性失败：`CELLBASE_MISSING`、`REWARD_FINALIZATION_NOT_READY`、`REWARD_BELOW_OCCUPIED_CAPACITY`、`CELLBASE_REWARD_MISMATCH`。

#### `check_cellbase_reward_target`

- 目标 lock 来源：目标块 cellbase 的第一个 witness，必须能解码成 `CellbaseWitness`，再从中读取 `lock`。
- PASS 条件：
  1. 仍先满足上面同样的目标高度、`finalized_at` 与 occupied-capacity 条件；
  2. 再把**当前块 cellbase 中 lock 与目标 lock 完全相同的输出容量**相加，这个和必须恰好等于 `expected_reward`。
- 边界说明：
  - 早期无奖励目标时，cellbase 应为空；
  - 即使当前实现的 cellbase 结构检查通常只允许一个输出，这里仍按“所有匹配目标 lock 的输出求和”的代码路径比较；
  - 如果当前块有奖励金额但没有任何输出支付到目标 lock，会报 `CELLBASE_REWARD_TARGET_MISSING` 或 `CELLBASE_REWARD_TARGET_MISMATCH`。
- 代表性失败：`CELLBASE_REWARD_TARGET_MISSING`、`CELLBASE_REWARD_TARGET_MISMATCH`、`REWARD_BELOW_OCCUPIED_CAPACITY`。

### 5.6 DAO 提现容量上限

#### `check_dao_withdraw_capacity`

- 适用：至少有一个输入引用的**源输出**满足：`type_` 存在，且 `hash_type == Type`、`code_hash == get_consensus.dao_type_hash`。
- DAO 输入识别：完全基于**源输出 type script**，不是看当前交易输出。
- 源输出 `data` 必须是**恰好 8 字节**，并按 little-endian 解码：
  - 若解码结果为 `0`：按 DAO 第一阶段提现处理，当前实现要求同输入索引位置存在 DAO withdrawing output，且其容量等于原始 deposit 容量、输出 data 记录源交易所在块高；这类输入按**原始 deposit 容量**计入 `dao_effective_sum`，不会调用 `calculate_dao_maximum_withdraw(...)`。
  - 若解码结果大于 `0`：按 DAO 最终提现处理，当前实现从该 withdrawing cell 的**源交易同索引输入**追溯 deposit out point，并从当前交易 `witness.input_type` 指向的 `header_dep` 校验 deposit 块高；校验通过后，再调用 `calculate_dao_maximum_withdraw(...)` 计算这笔输入的最大可提容量。
- 比较公式：`ordinary_output_sum <= ordinary_input_sum + dao_effective_sum`，单位 shannon。
  - 其中 `ordinary_input_sum` 是同交易里所有**已解析且非 DAO**输入来源输出容量之和；
  - `ordinary_output_sum` 按当前实现仍是**该交易全部输出容量之和**。
- 代表性失败：`DAO_INPUT_DATA_INVALID`、`DAO_WITHDRAWING_OUTPUT_CAPACITY_MISMATCH`、`DAO_DEPOSIT_HEADER_BLOCK_NUMBER_MISMATCH`、`DAO_WITHDRAW_CAPACITY_EXCEEDED`。
- 常见未完成原因：`DAO_MAXIMUM_WITHDRAW_MISSING`、`DAO_MAXIMUM_WITHDRAW_RPC_ERROR`、`DAO_DEPOSIT_HEADER_MISSING`、`DAO_WITHDRAW_CAPACITY_INCOMPLETE`、`DAO_CLASSIFICATION_UNKNOWN`。

---

## 6. 构建与测试

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
cargo build --release --locked
```
