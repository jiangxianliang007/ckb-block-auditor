# ckb-block-auditor

CKB Block Auditor V1（独立外部审计器），仅通过 CKB JSON-RPC 拉取数据并审计**新到达的规范链区块**。

## 目标与边界

- 仅使用 JSON-RPC（只读）
- 不访问节点数据库
- 不修改 CKB 节点源码
- 不执行完整 VM、不过度重实现共识
- 首次启动锚定当前 tip，不回扫历史
- 运行中断/重启后从持久化游标补齐漏块
- 识别同高/深度重组，回退到共同祖先后重审替换分支

## 架构

- `src/main.rs`：CLI 参数与进程启动
- `src/lib.rs`：
  - RPC 客户端（重试/超时）
  - 游标持久化（`CursorState`）
  - 轮询与重组处理
  - 区块/交易/经济检查
  - 单条 JSON 审计日志输出

## 运行

```bash
cargo run -- \
  --rpc-url http://127.0.0.1:8114 \
  --network mainnet \
  --node-id ckb-node-01 \
  --cursor-path ./data/cursor.json \
  --poll-interval-ms 3000
```

可选日志文件：

```bash
cargo run -- --log-path ./audit.log
```

## 配置项（核心）

- `--rpc-url`：CKB JSON-RPC 地址
- `--network`：网络标识（mainnet/testnet，自定义字符串可）
- `--node-id`：节点标识（用于日志）
- `--cursor-path`：游标文件路径
- `--poll-interval-ms`：轮询间隔
- `--rpc-timeout-secs` / `--max-retries`：RPC 超时与重试
- `--max-details`：单块 details 上限（超限会标记 `details_truncated=true`）
- `--dao-type-hash`：DAO type hash（用于识别 DAO 输入）

> 注意：不要把 RPC 凭据写入日志；本程序只记录网络/节点标识，不记录密钥信息。

## 审计日志规范

- 每个区块审计尝试仅输出 **一条 JSON**（stdout 或指定文件）
- 失败/未知明细进入 `details[]`
- 状态值：`PASS` / `FAIL` / `UNKNOWN` / `NOT_APPLICABLE` / `NOT_IMPLEMENTED`
- 汇总结果：
  - 任意确定性失败 => `FAIL`
  - 无失败但存在 required unknown => `INCOMPLETE`
  - 否则 => `PASS_WITHIN_SCOPE`
- `coverage` 固定 `PARTIAL`（V1 不是全共识证明器）

## 已实现检查（V1）

- 区块头/区块体：
  - `check_block_height`
  - `check_parent_hash`
  - `check_epoch_continuity`
  - `check_timestamp`（祖先中位时间 + 未来时间窗）
  - `check_block_size`（`serialized_size_without_uncle_proposals`）
  - `check_proposal_limit`
  - `check_block_hash`
  - `check_transaction_hashes`
  - `check_transactions_root`
  - `check_proposals_hash`
  - `check_extra_hash`
  - `check_duplicate_transactions`
  - `check_duplicate_proposals`
  - `check_cellbase_structure`
- 交易：
  - `check_transaction_version`
  - `check_inputs_outputs_structure`
  - `check_outputs_data_length`
  - `check_output_lock_hash_type`
  - `check_duplicate_cell_deps`
  - `check_duplicate_header_deps`
  - `check_duplicate_inputs_in_transaction`
  - `check_duplicate_inputs_in_block`
  - `check_input_content_resolution`
  - `check_input_output_index`
  - `check_occupied_capacity`
  - `check_ordinary_capacity_conservation`（普通交易输出<=输入）
- 经济一致性：
  - `check_cellbase_reward_amount`（基于 `get_block_economic_state` 一致性）
  - `check_cellbase_reward_target`（cellbase witness lock 与输出锁匹配）
  - `check_dao_withdraw_capacity`（DAO 输入按 `calculate_dao_maximum_withdraw` 计入有效输入容量）

## 明确未覆盖/部分覆盖

以下在 V1 保留 `NOT_IMPLEMENTED` 或部分覆盖：

- PoW/难度目标完整验证
- 两阶段提交窗口完整验证
- since / maturity 完整验证
- MMR / extension 额外共识规则
- VM 脚本执行与 cycles 验证
- 历史 live-cell 真正历史可花费性（仅做输入内容解析，不证明历史时点未花费）
- 独立重算完整发行/DAO 经济模型（V1 为 node RPC consistency）

## 信任边界

| 检查 | 方式 | 信任假设 |
|---|---|---|
| 哈希/根/结构类 | 本地重算对比 | 低（主要依赖返回原始区块内容） |
| 普通容量守恒 | 历史输入交易拉取 + 本地整数计算 | 中 |
| Cellbase 奖励 | `get_block_economic_state` 一致性 | 中-高（信任节点经济状态 RPC） |
| DAO 提现容量 | `calculate_dao_maximum_withdraw` 一致性 | 中-高（信任节点 DAO 计算 RPC） |

## 首启/重启/重组行为

- 首次启动：读取 tip，写入游标，**不扫历史块**
- 常规轮询：从游标后一高开始补齐到 tip
- 重启恢复：读取游标后继续补齐
- 重组：若游标高度 hash 与当前规范链不一致，向后回退到已保存历史中的共同祖先，重审替换分支
- 超出保存历史深度的重组：记录 stderr 警告并在下一轮重新锚定（不会静默当作 PASS）

## 审计日志示例（PASS）

```json
{"timestamp":"2026-09-11T08:30:01.250Z","schema_version":1,"service":"ckb-block-auditor","auditor_version":"0.1.0","network":"mainnet","node_id":"ckb-node-01","block_height":123,"block_hash":"0x...","parent_hash":"0x...","block_timestamp":1726043401000,"canonical_at_audit":true,"result":"PASS_WITHIN_SCOPE","coverage":"PARTIAL","audit_duration_ms":12,"check_block_height":"PASS","check_parent_hash":"PASS","check_epoch_continuity":"PASS","check_timestamp":"PASS","check_block_size":"PASS","check_proposal_limit":"PASS","check_block_hash":"PASS","check_transaction_hashes":"PASS","check_transactions_root":"PASS","check_proposals_hash":"PASS","check_extra_hash":"PASS","check_duplicate_transactions":"PASS","check_duplicate_proposals":"PASS","check_cellbase_structure":"PASS","check_transaction_version":"PASS","check_inputs_outputs_structure":"PASS","check_outputs_data_length":"PASS","check_output_lock_hash_type":"PASS","check_duplicate_cell_deps":"PASS","check_duplicate_header_deps":"PASS","check_duplicate_inputs_in_transaction":"PASS","check_duplicate_inputs_in_block":"PASS","check_input_content_resolution":"PASS","check_input_output_index":"PASS","check_occupied_capacity":"PASS","check_ordinary_capacity_conservation":"PASS","check_input_historical_liveness":"NOT_IMPLEMENTED","check_cellbase_reward_amount":"PASS","check_cellbase_reward_target":"PASS","check_dao_withdraw_capacity":"NOT_APPLICABLE","check_pow":"NOT_IMPLEMENTED","check_expected_epoch_target":"NOT_IMPLEMENTED","check_two_phase_commit":"NOT_IMPLEMENTED","check_cellbase_maturity":"NOT_IMPLEMENTED","check_since":"NOT_IMPLEMENTED","check_extension_consensus_rules":"NOT_IMPLEMENTED","check_vm_scripts":"NOT_IMPLEMENTED","check_cycles":"NOT_IMPLEMENTED","reward_verification_method":"node_rpc_consistency","dao_verification_method":"node_rpc_consistency","reward_target_block_hash":"0x...","reward_expected_amount":"50000000000","reward_actual_amount":"50000000000","tx_count":2,"proposal_count":0,"uncle_count":0,"block_consensus_size_bytes":1234,"block_consensus_size_limit_bytes":597688320,"capacity_eligible_tx_count":1,"capacity_checked_tx_count":1,"capacity_failed_tx_count":0,"capacity_unchecked_tx_count":0,"dao_related_tx_count":0,"dao_checked_inputs_count":0,"dao_unresolved_inputs_count":0,"unresolved_input_count":0,"failed_checks":[],"unknown_checks":[],"details_truncated":false,"details_total":0,"details":[]}
```

## 审计日志示例（FAIL）

```json
{"timestamp":"2026-09-11T08:31:01.250Z","schema_version":1,"service":"ckb-block-auditor","auditor_version":"0.1.0","network":"mainnet","node_id":"ckb-node-01","block_height":124,"block_hash":"0x...","parent_hash":"0x...","block_timestamp":1726043461000,"canonical_at_audit":true,"result":"FAIL","coverage":"PARTIAL","audit_duration_ms":18,"check_ordinary_capacity_conservation":"FAIL","failed_checks":["check_ordinary_capacity_conservation"],"unknown_checks":[],"details_truncated":false,"details_total":1,"details":[{"check_name":"check_ordinary_capacity_conservation","status":"FAIL","error_code":"OUTPUT_CAPACITY_EXCEEDS_INPUT","tx_hash":"0x...","tx_index":1,"input_index":null,"output_index":null,"expected_operator":"less_than_or_equal","expected_value":"100","actual_value":"110","unit":"shannon","reason":"ordinary outputs must be <= ordinary inputs"}]}
```

## 外部采集建议

- stdout 单行 JSON 可直接被日志代理采集（如 Fluent Bit / Vector）后写入 OpenObserve
- 建议额外监控：
  - 进程存活
  - “连续 N 分钟没有新区块审计日志”告警（区分无新区块与进程故障）

## 测试与质量

```bash
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

当前测试覆盖：

- epoch 连续性基础逻辑
- 游标持久化读写
- 首次启动锚定 tip（不回扫历史）
- 普通交易输出>输入触发 FAIL
