# ckb-block-auditor

CKB Block Auditor V1.2（当前 crate 版本 `0.2.0`）。  
只通过 CKB JSON-RPC 审计**当前连接 RPC 所在链**的规范链区块；每个区块输出**一条单行 JSON**，便于外部采集。

## 边界

- 仅使用 JSON-RPC（只读）
- 不访问节点数据库
- 不修改 `nervosnetwork/ckb`
- `coverage` 固定为 `PARTIAL`
- 自动调用 `get_consensus` 读取校验所需参数（区块大小、proposal limit、tx version、DAO type hash、reward finalization window 等）
- **不再需要** `--network` / `CKB_NETWORK`，也不会把 `get_consensus.id` 当作必须手填的等值参数
- reward / DAO 当前仍是 **node RPC consistency check**：依赖同节点返回的 `get_consensus`、`get_block_economic_state`、`calculate_dao_maximum_withdraw` 等数据做一致性审计，**不是**独立重算完整共识经济学 / 完整 VM 证明
- 不做完整 VM / cycles / PoW / since / maturity / two-phase commit / 全历史 live-cell 证明
- 对兼容 CKB RPC 与共识字段的网络可工作，但**不会声称任意改造链都完整支持**

## 运行

实时模式（不落盘 cursor，启动时先审计当前 tip，此后只跟随新块；重启后**不补停机期间区块**）：

```bash
cargo run -- \
  --rpc-url http://127.0.0.1:8114 \
  --node-id ckb-node-01 \
  --log-path ./audit.log \
  --poll-interval-ms 3000
```

持久化模式（显式指定 cursor，文件不存在时从当前 tip 开始审计并创建文件；文件已存在时断点续审并补齐停机期间漏块）：

```bash
cargo run -- \
  --rpc-url https://mainnet.ckbapp.dev \
  --node-id ckb-mainnet-01 \
  --cursor-path ./data/mainnet.cursor.json \
  --log-path ./audit-mainnet.log \
  --poll-interval-ms 3000
```

二进制：

```bash
cargo build --release --locked
./target/release/ckb-block-auditor \
  --rpc-url https://mainnet.ckbapp.dev \
  --node-id ckb-mainnet-01 \
  --cursor-path ./data/mainnet.cursor.json \
  --log-path ./audit-mainnet.log
```

## 参数

| 参数 | 说明 |
|---|---|
| `--rpc-url` | CKB JSON-RPC 地址 |
| `--node-id` | 写入日志的实例标识 |
| `--cursor-path` | 可选；显式启用持久化 cursor。也可通过 `CKB_CURSOR_PATH` 环境变量显式设置 |
| `--log-path` | 可选；默认输出到 stdout |
| `--poll-interval-ms` | 轮询间隔 |
| `--dao-type-hash` | 可选一致性覆盖值；默认以节点 `get_consensus` 返回值为准 |

## Cursor 行为

### 1）未设置 `--cursor-path` / `CKB_CURSOR_PATH`

- 不读取、不创建、不写入任何默认 `cursor.json`
- 启动时先审计**当前规范链 tip**
- 运行期间使用**内存 cursor/history** 去重、补齐轮询间隔内新增的多个区块、处理有限深度重组
- 重启后重新锚定并审计新的当前 tip；**不会自动回放停机期间历史**

### 2）设置了 `--cursor-path`

- 文件不存在：从当前 tip 开始审计，然后创建并持续原子写入 cursor
- 文件存在：从保存进度继续，补齐停机期间漏块
- cursor 自动绑定 `genesis_hash`
- 同一条链上更换 RPC 可以继续使用同一 cursor
- **不同链必须使用不同 cursor 文件**
- 若 cursor 的 `genesis_hash` 与当前 RPC 的实际 genesis 不一致，会明确报错，且**不会修改原文件**

### 3）旧版 cursor（没有 `genesis_hash`）

- 仅在**已有 history 与当前 RPC 的规范链记录全部匹配**时，才会安全迁移并写回 `genesis_hash`
- 无法安全确认时，会报出**非破坏性错误**，请改用新的 cursor 文件

## 日志语义

- 每个区块仅一条单行 JSON
- `result` 优先级：`FAIL` > `INCOMPLETE` > `PASS_WITHIN_SCOPE`
- `schema_version = 3`
- `auditor_version = 0.2.0`
- 仍然省略 `NOT_APPLICABLE` / `NOT_IMPLEMENTED` 的 `check_*`
- 成功日志默认只保留身份、上下文、总体结果、适用检查状态
- 异常时输出：
  - `failed_checks`
  - `unknown_checks`
  - `details_total`
  - `details_truncated`
  - `details`
- `details_total` 表示**发现的异常明细总数**
- `details` 最多保留 `max_details` 条；若超出则 `details_truncated=true`
- 即使明细被截断，`failed_checks` / `unknown_checks` 仍会汇总所有已检测到的异常检查项

## 日志示例

PASS：

```json
{"timestamp":"2026-09-11T10:30:01.250Z","schema_version":3,"service":"ckb-block-auditor","auditor_version":"0.2.0","node_id":"ckb-node-01","block_height":123,"block_hash":"0x...","parent_hash":"0x...","block_timestamp":1726043401000,"canonical_at_audit":true,"result":"PASS_WITHIN_SCOPE","coverage":"PARTIAL","audit_duration_ms":12,"check_block_height":"PASS","check_parent_hash":"PASS","check_epoch_continuity":"PASS","check_timestamp":"PASS","check_block_size":"PASS","check_proposal_limit":"PASS","check_block_hash":"PASS","check_transaction_hashes":"PASS","check_transactions_root":"PASS","check_proposals_hash":"PASS","check_extra_hash":"PASS","check_duplicate_transactions":"PASS","check_duplicate_proposals":"PASS","check_cellbase_structure":"PASS","check_cellbase_reward_amount":"PASS","check_cellbase_reward_target":"PASS"}
```

FAIL / UNKNOWN：

```json
{"timestamp":"2026-09-11T10:31:01.250Z","schema_version":3,"service":"ckb-block-auditor","auditor_version":"0.2.0","node_id":"ckb-node-01","block_height":124,"block_hash":"0x...","parent_hash":"0x...","block_timestamp":1726043461000,"canonical_at_audit":false,"result":"FAIL","coverage":"PARTIAL","audit_duration_ms":18,"check_block_size":"FAIL","check_transaction_hashes":"FAIL","check_transactions_root":"FAIL","check_input_content_resolution":"UNKNOWN","check_input_output_index":"UNKNOWN","check_ordinary_capacity_conservation":"UNKNOWN","check_dao_withdraw_capacity":"UNKNOWN","check_cellbase_reward_amount":"UNKNOWN","check_cellbase_reward_target":"UNKNOWN","failed_checks":["check_block_size","check_transaction_hashes","check_transactions_root"],"unknown_checks":["check_input_content_resolution","check_input_output_index","check_ordinary_capacity_conservation","check_dao_withdraw_capacity","check_cellbase_reward_amount","check_cellbase_reward_target"],"details_truncated":false,"details_total":4,"details":[{"check_name":"check_block_size","status":"FAIL","error_code":"BLOCK_SIZE_EXCEEDED","expected_operator":"less_than_or_equal","expected_value":"597000","actual_value":"600123","unit":"byte","reason":"block serialized size exceeds the consensus max_block_bytes limit"},{"check_name":"check_input_content_resolution","status":"UNKNOWN","error_code":"INPUT_TX_MISSING","tx_hash":"0x...","tx_index":1,"input_index":0,"referenced_out_point":"0x...:0","reason":"get_transaction returned null"},{"check_name":"check_input_output_index","status":"UNKNOWN","error_code":"INPUT_INDEX_UNVERIFIED","tx_hash":"0x...","tx_index":1,"input_index":0,"referenced_out_point":"0x...:0","reason":"input output index could not be verified because the source transaction was unavailable"},{"check_name":"check_cellbase_reward_amount","status":"UNKNOWN","error_code":"ECONOMIC_STATE_MISSING","reason":"get_block_economic_state returned null for reward target block 20422085 (0x...)"}]}
```

## 已实现检查

| 类别 | check 字段 | 当前实现 |
|---|---|---|
| 区块头/块体 | `check_block_height` | 父块高度 +1 |
| 区块头/块体 | `check_parent_hash` | 本地重算父块 header hash |
| 区块头/块体 | `check_epoch_continuity` | successor / well-formed 语义 |
| 区块头/块体 | `check_timestamp` | 使用 `get_consensus.median_time_block_count` 回看祖先中位时间 |
| 区块头/块体 | `check_block_size` | 使用 `get_consensus.max_block_bytes` |
| 区块头/块体 | `check_proposal_limit` | 使用 `get_consensus.max_block_proposals_limit` |
| 区块头/块体 | `check_block_hash` / `check_transaction_hashes` / `check_transactions_root` / `check_proposals_hash` / `check_extra_hash` | 本地重算 |
| 区块头/块体 | `check_duplicate_transactions` / `check_duplicate_proposals` | 重复检测 |
| Cellbase | `check_cellbase_structure` | 唯一性、输入、witness、空 output data |
| 交易结构 | `check_transaction_version` | 使用 `get_consensus.tx_version` |
| 交易结构 | `check_inputs_outputs_structure` / `check_outputs_data_length` | 结构检查 |
| 交易结构 | `check_duplicate_cell_deps` / `check_duplicate_header_deps` / `check_duplicate_inputs_in_transaction` / `check_duplicate_inputs_in_block` | 重复检测 |
| 输入解析 | `check_input_content_resolution` | `get_transaction` 已提交 JSON、一致性与解码校验 |
| 输入解析 | `check_input_output_index` | 依赖源交易输出索引有效性 |
| 容量 | `check_occupied_capacity` | 输出占用容量 |
| 容量 | `check_ordinary_capacity_conservation` | 仅对可明确分类的普通交易判定；缺输入/分类不明则 `UNKNOWN` |
| Reward | `check_cellbase_reward_amount` | 基于奖励目标块的 `get_block_economic_state` 一致性 |
| Reward | `check_cellbase_reward_target` | 使用奖励目标块 cellbase witness lock 对比 |
| DAO | `check_dao_withdraw_capacity` | 对可明确解析的 DAO 场景调用 `calculate_dao_maximum_withdraw` |

## 未实现 / 部分覆盖

| 范围 | 状态 |
|---|---|
| PoW、完整难度目标、完整 epoch target | 未实现，不写入每块 JSON |
| two-phase commit、cellbase maturity、since | 未实现，不写入每块 JSON |
| extension、VM scripts、cycles | 未实现，不写入每块 JSON |
| 全历史 input liveness | 未实现；不会假装 PASS |
| reward / DAO 独立共识重算 | 未实现；当前仅校验节点 RPC 返回与链上数据是否自洽 |
| 复杂 / 歧义 DAO 提现追踪 | 诚实输出 `UNKNOWN` |

## 构建与测试

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
cargo build --release --locked
```

当前回归覆盖包含：

- CLI 去除 `--network` 与 `--cursor-path` 可选行为
- `get_consensus.id` 不再要求与手工网络名相等；同链不同 RPC / 不同 consensus id 可继续使用 genesis 绑定的 cursor
- 缺少 consensus 时不消费区块进度、不创建 cursor
- 无 cursor 文件模式下：启动先审计当前 tip、轮询间补齐多个新区块、不重复输出、重启重新选择当前 tip
- 持久化 cursor 的 genesis mismatch 防护与 legacy cursor 安全迁移
- 成功 / 失败日志序列化精简、异常汇总与明细截断
- reward 目标块 lock 校验、普通容量失败、unresolved input 的 `UNKNOWN` 可见性
