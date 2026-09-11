# ckb-block-auditor

CKB Block Auditor V1.1（独立外部审计器）。  
仅通过 CKB JSON-RPC 审计**新到达的规范链区块**，每个区块输出**一条单行 JSON**，便于外部采集到 OpenObserve。

## 边界

- 仅使用 JSON-RPC（只读）
- 不访问节点数据库
- 不修改 `nervosnetwork/ckb`
- 首次启动锚定当前 tip，**不回扫全历史**
- 重启后从游标继续补齐漏块
- 识别并回退同高/有限深度重组
- 不做完整 VM / cycles / PoW / since / maturity / 两阶段提交 / 全历史 live-cell 证明
- reward / DAO 目前是 **node RPC consistency check**，不是独立重算完整共识经济学

## 运行

```bash
cargo run -- \
  --rpc-url http://127.0.0.1:8114 \
  --network mainnet \
  --node-id ckb-node-01 \
  --cursor-path ./data/cursor.json \
  --log-path ./audit.log \
  --poll-interval-ms 3000
```

二进制：

```bash
cargo build --release --locked
./target/release/ckb-block-auditor \
  --rpc-url https://mainnet.ckbapp.dev \
  --network mainnet \
  --node-id ckb-mainnet-01 \
  --cursor-path ./data/cursor.json \
  --log-path ./audit.log \
  --poll-interval-ms 3000
```

## 构建说明

- 仓库带有 `Cargo.lock`，建议优先使用 `cargo build --locked` / `cargo test --locked`
- 本项目使用 Rust 2024 edition；如果本地工具链或依赖版本不兼容，优先升级到较新的 stable 工具链
- 如果必须固定旧依赖版本，再按 Cargo 报错提示使用 `cargo update <name>@<current-ver> --precise <compatible-ver>`

## 核心参数

| 参数 | 说明 |
|---|---|
| `--rpc-url` | CKB JSON-RPC 地址 |
| `--network` | 必须与节点 `get_consensus.id` 一致 |
| `--node-id` | 写入日志的节点标识 |
| `--cursor-path` | 游标文件 |
| `--log-path` | 可选，单行 JSON 日志文件 |
| `--poll-interval-ms` | 轮询间隔 |

额外兼容参数：

- `--dao-type-hash`：可选一致性校验覆盖值；默认改为留空，实际以节点 `get_consensus` 返回值为准

## 日志语义

- 每个区块仅一条 JSON
- `result` 优先级：`FAIL` > `INCOMPLETE` > `PASS_WITHIN_SCOPE`
- `coverage` 固定为 `PARTIAL`
- `auditor_version` 用于区分实现版本
- `schema_version = 2`
- `details[]` 只记录 `FAIL` / `UNKNOWN`
- **不会输出** `NOT_IMPLEMENTED` 或 `NOT_APPLICABLE` 的 `check_*` 字段
- 因此：**字段缺失 ≠ PASS**，只表示“未报告 / 不适用 / 当前版本未实现”
- 真正执行但拿不到足够数据时会显式输出 `UNKNOWN`

## 已实现检查（V1.1）

| 类别 | check 字段 | 当前实现 |
|---|---|---|
| 区块头/块体 | `check_block_height` | 父块高度 +1 |
| 区块头/块体 | `check_parent_hash` | 本地重算父块 header hash |
| 区块头/块体 | `check_epoch_continuity` | 使用 well-formed / successor 语义 |
| 区块头/块体 | `check_timestamp` | 使用节点 `get_consensus.median_time_block_count` 取祖先中位时间；祖先不完整则 `UNKNOWN` |
| 区块头/块体 | `check_block_size` | 使用共识 `max_block_bytes` 比较 molecule 区块大小 |
| 区块头/块体 | `check_proposal_limit` | 使用共识 `max_block_proposals_limit` |
| 区块头/块体 | `check_block_hash` / `check_transaction_hashes` / `check_transactions_root` / `check_proposals_hash` / `check_extra_hash` | 本地重算 |
| 区块头/块体 | `check_duplicate_transactions` / `check_duplicate_proposals` | 重复检测 |
| Cellbase | `check_cellbase_structure` | cellbase 唯一性、输入、witness、空 output data |
| 交易结构 | `check_transaction_version` | 使用共识 `tx_version` |
| 交易结构 | `check_inputs_outputs_structure` / `check_outputs_data_length` | 结构检查 |
| 交易结构 | `check_output_lock_hash_type` | 不再把 `Data2` 误判为失败 |
| 交易结构 | `check_duplicate_cell_deps` / `check_duplicate_header_deps` / `check_duplicate_inputs_in_transaction` / `check_duplicate_inputs_in_block` | 重复检测 |
| 输入解析 | `check_input_content_resolution` | 验证 `get_transaction` 全量 committed JSON、状态、解码、重算 tx hash |
| 输入解析 | `check_input_output_index` | 依赖未解析时不再误报 PASS |
| 容量 | `check_occupied_capacity` | 输出占用容量 |
| 容量 | `check_ordinary_capacity_conservation` | 仅对可明确分类的普通交易判定；缺输入/分类不明保持 `UNKNOWN` |
| Reward | `check_cellbase_reward_amount` | 基于**奖励目标块**的 `get_block_economic_state` 一致性；不足数据时 `UNKNOWN` |
| Reward | `check_cellbase_reward_target` | 使用**奖励目标块 cellbase witness lock** 对比，不再错误比较当前块 witness |
| DAO | `check_dao_withdraw_capacity` | 对可明确解析的 DAO 场景调用 `calculate_dao_maximum_withdraw`；歧义场景 `UNKNOWN` |

## 未实现 / 部分覆盖

| 范围 | 状态 |
|---|---|
| PoW、难度目标、完整 epoch target | 未实现，不写入每块 JSON |
| two-phase commit、cellbase maturity、since | 未实现，不写入每块 JSON |
| extension、VM scripts、cycles | 未实现，不写入每块 JSON |
| 全历史 input liveness | 未实现；不会假装 PASS |
| reward / DAO 独立共识重算 | **未实现**；当前仅验证节点 RPC 返回与链上数据是否自洽 |
| 复杂/歧义 DAO 提现追踪 | 诚实输出 `UNKNOWN`，不误报 `PASS` / `NOT_APPLICABLE` |

## 信任边界

| 检查 | 数据来源 | 说明 |
|---|---|---|
| 哈希、根、结构、重复项 | 区块 RPC + 本地重算 | 低信任假设 |
| 普通容量守恒 | `get_transaction` + 本地整数计算 | 依赖节点返回已提交源交易 |
| Cellbase reward | `get_consensus` + `get_block_economic_state` + 目标块 `get_block` | 同节点一致性检查 |
| DAO 提现容量 | `get_consensus` + `get_transaction` + `calculate_dao_maximum_withdraw` | 同节点一致性检查 |

## 日志示例

PASS（省略了未实现/不适用字段）：

```json
{"timestamp":"2026-09-11T08:30:01.250Z","schema_version":2,"service":"ckb-block-auditor","auditor_version":"0.1.1","network":"mainnet","node_id":"ckb-node-01","block_height":123,"block_hash":"0x...","parent_hash":"0x...","block_timestamp":1726043401000,"canonical_at_audit":true,"result":"PASS_WITHIN_SCOPE","coverage":"PARTIAL","audit_duration_ms":12,"check_block_height":"PASS","check_parent_hash":"PASS","check_epoch_continuity":"PASS","check_timestamp":"PASS","check_block_size":"PASS","check_proposal_limit":"PASS","check_block_hash":"PASS","check_transaction_hashes":"PASS","check_transactions_root":"PASS","check_proposals_hash":"PASS","check_extra_hash":"PASS","check_duplicate_transactions":"PASS","check_duplicate_proposals":"PASS","check_cellbase_structure":"PASS","check_cellbase_reward_amount":"PASS","check_cellbase_reward_target":"PASS","reward_verification_method":"node_rpc_consistency","dao_verification_method":"node_rpc_consistency","reward_target_block_hash":"0x...","reward_expected_amount":"63412170556","reward_actual_amount":"63412170556","tx_count":1,"proposal_count":0,"uncle_count":0,"block_consensus_size_bytes":670,"block_consensus_size_limit_bytes":123456789,"capacity_eligible_tx_count":0,"capacity_checked_tx_count":0,"capacity_failed_tx_count":0,"capacity_unchecked_tx_count":0,"dao_related_tx_count":0,"dao_checked_inputs_count":0,"dao_unresolved_inputs_count":0,"unresolved_input_count":0,"failed_checks":[],"unknown_checks":[],"details_truncated":false,"details_total":0,"details":[]}
```

UNKNOWN / FAIL：

```json
{"timestamp":"2026-09-11T08:31:01.250Z","schema_version":2,"service":"ckb-block-auditor","auditor_version":"0.1.1","network":"mainnet","node_id":"ckb-node-01","block_height":124,"block_hash":"0x...","parent_hash":"0x...","block_timestamp":1726043461000,"canonical_at_audit":false,"result":"INCOMPLETE","coverage":"PARTIAL","audit_duration_ms":18,"check_input_content_resolution":"UNKNOWN","check_input_output_index":"UNKNOWN","check_ordinary_capacity_conservation":"UNKNOWN","check_dao_withdraw_capacity":"UNKNOWN","check_cellbase_reward_amount":"UNKNOWN","check_cellbase_reward_target":"UNKNOWN","reward_verification_method":"node_rpc_consistency","dao_verification_method":"node_rpc_consistency","reward_actual_amount":"63410906051","tx_count":4,"proposal_count":0,"uncle_count":0,"block_consensus_size_bytes":3350,"block_consensus_size_limit_bytes":123456789,"capacity_eligible_tx_count":3,"capacity_checked_tx_count":0,"capacity_failed_tx_count":0,"capacity_unchecked_tx_count":3,"dao_related_tx_count":0,"dao_checked_inputs_count":0,"dao_unresolved_inputs_count":0,"unresolved_input_count":9,"failed_checks":[],"unknown_checks":["check_input_content_resolution","check_input_output_index","check_ordinary_capacity_conservation","check_dao_withdraw_capacity","check_cellbase_reward_amount","check_cellbase_reward_target"],"details_truncated":false,"details_total":2,"details":[{"check_name":"check_input_content_resolution","status":"UNKNOWN","error_code":"INPUT_TX_MISSING","tx_hash":"0x...","tx_index":1,"input_index":0,"referenced_out_point":"0x...:0","reason":"get_transaction returned null"},{"check_name":"check_cellbase_reward_amount","status":"UNKNOWN","error_code":"ECONOMIC_STATE_MISSING","reason":"get_block_economic_state returned null for reward target block 20422085 (0x...)"}]}
```

## 测试

```bash
cargo fmt --check
cargo check --locked
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
```

当前回归测试覆盖：

- `get_transaction` 正确 HTTP JSON 参数序列化
- HTTP 错误不泄露 URL 凭据
- 共识 `max_block_bytes` 边界
- reward 使用目标块而不是当前块 witness
- unresolved input 时 `UNKNOWN` 可见，且不会把 DAO 误记为 `NOT_APPLICABLE`
- 序列化省略 `NOT_IMPLEMENTED` / `NOT_APPLICABLE`
