use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use ckb_jsonrpc_types::{
    BlockEconomicState, BlockView, Either, HeaderView, OutPoint, ResponseFormat, ScriptHashType,
    TransactionView, TransactionWithStatusResponse, Uint64,
};
use ckb_occupied_capacity::Capacity as OccupiedCapacity;
use ckb_types::H256;
use ckb_types::core::{BlockView as CoreBlockView, EpochNumberWithFraction};
use ckb_types::packed;
use ckb_types::prelude::*;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone)]
pub struct AuditorConfig {
    pub rpc_url: String,
    pub network: String,
    pub node_id: String,
    pub poll_interval_ms: u64,
    pub rpc_timeout_secs: u64,
    pub max_retries: u32,
    pub cursor_path: PathBuf,
    pub log_path: Option<PathBuf>,
    pub max_details: usize,
    pub max_future_ms: u64,
    pub median_time_span: usize,
    pub proposal_limit: usize,
    pub tx_version: u32,
    pub history_retention: usize,
    pub dao_type_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorState {
    pub last_height: u64,
    pub last_hash: String,
    pub history: BTreeMap<u64, String>,
}

impl CursorState {
    pub async fn load(path: &Path) -> Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }
        let content = tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("failed to read cursor file {}", path.display()))?;
        Ok(Some(
            serde_json::from_str(&content).context("invalid cursor file")?,
        ))
    }

    pub async fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create cursor dir {}", parent.display()))?;
        }
        tokio::fs::write(path, serde_json::to_vec_pretty(self)?)
            .await
            .with_context(|| format!("failed to write cursor file {}", path.display()))
    }

    fn push_block(&mut self, height: u64, hash: String, retention: usize) {
        self.last_height = height;
        self.last_hash = hash.clone();
        self.history.insert(height, hash);
        while self.history.len() > retention {
            if let Some(key) = self.history.keys().next().copied() {
                self.history.remove(&key);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CheckStatus {
    Pass,
    Fail,
    Unknown,
    NotApplicable,
    NotImplemented,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AuditResult {
    Fail,
    Incomplete,
    PassWithinScope,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailItem {
    pub check_name: String,
    pub status: CheckStatus,
    pub error_code: String,
    pub tx_hash: Option<String>,
    pub tx_index: Option<usize>,
    pub input_index: Option<usize>,
    pub output_index: Option<usize>,
    pub expected_operator: Option<String>,
    pub expected_value: Option<String>,
    pub actual_value: Option<String>,
    pub unit: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLog {
    pub timestamp: String,
    pub schema_version: u32,
    pub service: String,
    pub auditor_version: String,
    pub network: String,
    pub node_id: String,

    pub block_height: u64,
    pub block_hash: String,
    pub parent_hash: String,
    pub block_timestamp: u64,
    pub canonical_at_audit: bool,

    pub result: AuditResult,
    pub coverage: String,
    pub audit_duration_ms: u64,

    pub check_block_height: CheckStatus,
    pub check_parent_hash: CheckStatus,
    pub check_epoch_continuity: CheckStatus,
    pub check_timestamp: CheckStatus,
    pub check_block_size: CheckStatus,
    pub check_proposal_limit: CheckStatus,
    pub check_block_hash: CheckStatus,
    pub check_transaction_hashes: CheckStatus,
    pub check_transactions_root: CheckStatus,
    pub check_proposals_hash: CheckStatus,
    pub check_extra_hash: CheckStatus,
    pub check_duplicate_transactions: CheckStatus,
    pub check_duplicate_proposals: CheckStatus,
    pub check_cellbase_structure: CheckStatus,

    pub check_transaction_version: CheckStatus,
    pub check_inputs_outputs_structure: CheckStatus,
    pub check_outputs_data_length: CheckStatus,
    pub check_output_lock_hash_type: CheckStatus,
    pub check_duplicate_cell_deps: CheckStatus,
    pub check_duplicate_header_deps: CheckStatus,
    pub check_duplicate_inputs_in_transaction: CheckStatus,
    pub check_duplicate_inputs_in_block: CheckStatus,
    pub check_input_content_resolution: CheckStatus,
    pub check_input_output_index: CheckStatus,
    pub check_occupied_capacity: CheckStatus,
    pub check_ordinary_capacity_conservation: CheckStatus,
    pub check_input_historical_liveness: CheckStatus,

    pub check_cellbase_reward_amount: CheckStatus,
    pub check_cellbase_reward_target: CheckStatus,
    pub check_dao_withdraw_capacity: CheckStatus,

    pub check_pow: CheckStatus,
    pub check_expected_epoch_target: CheckStatus,
    pub check_two_phase_commit: CheckStatus,
    pub check_cellbase_maturity: CheckStatus,
    pub check_since: CheckStatus,
    pub check_extension_consensus_rules: CheckStatus,
    pub check_vm_scripts: CheckStatus,
    pub check_cycles: CheckStatus,

    pub reward_verification_method: String,
    pub dao_verification_method: String,
    pub reward_target_block_hash: Option<String>,
    pub reward_expected_amount: Option<String>,
    pub reward_actual_amount: Option<String>,

    pub tx_count: usize,
    pub proposal_count: usize,
    pub uncle_count: usize,
    pub block_consensus_size_bytes: usize,
    pub block_consensus_size_limit_bytes: usize,

    pub capacity_eligible_tx_count: usize,
    pub capacity_checked_tx_count: usize,
    pub capacity_failed_tx_count: usize,
    pub capacity_unchecked_tx_count: usize,
    pub dao_related_tx_count: usize,
    pub dao_checked_inputs_count: usize,
    pub dao_unresolved_inputs_count: usize,
    pub unresolved_input_count: usize,

    pub failed_checks: Vec<String>,
    pub unknown_checks: Vec<String>,
    pub details_truncated: bool,
    pub details_total: usize,
    pub details: Vec<DetailItem>,
}

impl AuditLog {
    fn new(config: &AuditorConfig, block: &BlockView) -> Self {
        Self {
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            schema_version: 1,
            service: "ckb-block-auditor".to_string(),
            auditor_version: env!("CARGO_PKG_VERSION").to_string(),
            network: config.network.clone(),
            node_id: config.node_id.clone(),
            block_height: block.header.inner.number.value(),
            block_hash: format!("{:#x}", block.header.hash),
            parent_hash: format!("{:#x}", block.header.inner.parent_hash),
            block_timestamp: block.header.inner.timestamp.value(),
            canonical_at_audit: true,
            result: AuditResult::Incomplete,
            coverage: "PARTIAL".to_string(),
            audit_duration_ms: 0,

            check_block_height: CheckStatus::Unknown,
            check_parent_hash: CheckStatus::Unknown,
            check_epoch_continuity: CheckStatus::Unknown,
            check_timestamp: CheckStatus::Unknown,
            check_block_size: CheckStatus::Unknown,
            check_proposal_limit: CheckStatus::Unknown,
            check_block_hash: CheckStatus::Unknown,
            check_transaction_hashes: CheckStatus::Unknown,
            check_transactions_root: CheckStatus::Unknown,
            check_proposals_hash: CheckStatus::Unknown,
            check_extra_hash: CheckStatus::Unknown,
            check_duplicate_transactions: CheckStatus::Unknown,
            check_duplicate_proposals: CheckStatus::Unknown,
            check_cellbase_structure: CheckStatus::Unknown,

            check_transaction_version: CheckStatus::Unknown,
            check_inputs_outputs_structure: CheckStatus::Unknown,
            check_outputs_data_length: CheckStatus::Unknown,
            check_output_lock_hash_type: CheckStatus::Unknown,
            check_duplicate_cell_deps: CheckStatus::Unknown,
            check_duplicate_header_deps: CheckStatus::Unknown,
            check_duplicate_inputs_in_transaction: CheckStatus::Unknown,
            check_duplicate_inputs_in_block: CheckStatus::Unknown,
            check_input_content_resolution: CheckStatus::Unknown,
            check_input_output_index: CheckStatus::Unknown,
            check_occupied_capacity: CheckStatus::Unknown,
            check_ordinary_capacity_conservation: CheckStatus::Unknown,
            check_input_historical_liveness: CheckStatus::NotImplemented,

            check_cellbase_reward_amount: CheckStatus::Unknown,
            check_cellbase_reward_target: CheckStatus::Unknown,
            check_dao_withdraw_capacity: CheckStatus::NotApplicable,

            check_pow: CheckStatus::NotImplemented,
            check_expected_epoch_target: CheckStatus::NotImplemented,
            check_two_phase_commit: CheckStatus::NotImplemented,
            check_cellbase_maturity: CheckStatus::NotImplemented,
            check_since: CheckStatus::NotImplemented,
            check_extension_consensus_rules: CheckStatus::NotImplemented,
            check_vm_scripts: CheckStatus::NotImplemented,
            check_cycles: CheckStatus::NotImplemented,

            reward_verification_method: "node_rpc_consistency".to_string(),
            dao_verification_method: "node_rpc_consistency".to_string(),
            reward_target_block_hash: None,
            reward_expected_amount: None,
            reward_actual_amount: None,

            tx_count: block.transactions.len(),
            proposal_count: block.proposals.len(),
            uncle_count: block.uncles.len(),
            block_consensus_size_bytes: 0,
            block_consensus_size_limit_bytes: 0,
            capacity_eligible_tx_count: 0,
            capacity_checked_tx_count: 0,
            capacity_failed_tx_count: 0,
            capacity_unchecked_tx_count: 0,
            dao_related_tx_count: 0,
            dao_checked_inputs_count: 0,
            dao_unresolved_inputs_count: 0,
            unresolved_input_count: 0,
            failed_checks: vec![],
            unknown_checks: vec![],
            details_truncated: false,
            details_total: 0,
            details: vec![],
        }
    }

    fn push_detail(&mut self, config: &AuditorConfig, item: DetailItem) {
        self.details_total += 1;
        if self.details.len() < config.max_details {
            self.details.push(item);
        } else {
            self.details_truncated = true;
        }
    }

    fn finalize(&mut self) {
        let checks = vec![
            ("check_block_height", self.check_block_height),
            ("check_parent_hash", self.check_parent_hash),
            ("check_epoch_continuity", self.check_epoch_continuity),
            ("check_timestamp", self.check_timestamp),
            ("check_block_size", self.check_block_size),
            ("check_proposal_limit", self.check_proposal_limit),
            ("check_block_hash", self.check_block_hash),
            ("check_transaction_hashes", self.check_transaction_hashes),
            ("check_transactions_root", self.check_transactions_root),
            ("check_proposals_hash", self.check_proposals_hash),
            ("check_extra_hash", self.check_extra_hash),
            (
                "check_duplicate_transactions",
                self.check_duplicate_transactions,
            ),
            ("check_duplicate_proposals", self.check_duplicate_proposals),
            ("check_cellbase_structure", self.check_cellbase_structure),
            ("check_transaction_version", self.check_transaction_version),
            (
                "check_inputs_outputs_structure",
                self.check_inputs_outputs_structure,
            ),
            ("check_outputs_data_length", self.check_outputs_data_length),
            (
                "check_output_lock_hash_type",
                self.check_output_lock_hash_type,
            ),
            ("check_duplicate_cell_deps", self.check_duplicate_cell_deps),
            (
                "check_duplicate_header_deps",
                self.check_duplicate_header_deps,
            ),
            (
                "check_duplicate_inputs_in_transaction",
                self.check_duplicate_inputs_in_transaction,
            ),
            (
                "check_duplicate_inputs_in_block",
                self.check_duplicate_inputs_in_block,
            ),
            (
                "check_input_content_resolution",
                self.check_input_content_resolution,
            ),
            ("check_input_output_index", self.check_input_output_index),
            ("check_occupied_capacity", self.check_occupied_capacity),
            (
                "check_ordinary_capacity_conservation",
                self.check_ordinary_capacity_conservation,
            ),
            (
                "check_cellbase_reward_amount",
                self.check_cellbase_reward_amount,
            ),
            (
                "check_cellbase_reward_target",
                self.check_cellbase_reward_target,
            ),
            (
                "check_dao_withdraw_capacity",
                self.check_dao_withdraw_capacity,
            ),
        ];

        self.failed_checks = checks
            .iter()
            .filter_map(|(name, status)| {
                (*status == CheckStatus::Fail).then_some((*name).to_string())
            })
            .collect();
        self.unknown_checks = checks
            .iter()
            .filter_map(|(name, status)| {
                (*status == CheckStatus::Unknown).then_some((*name).to_string())
            })
            .collect();

        self.result = if !self.failed_checks.is_empty() {
            AuditResult::Fail
        } else if !self.unknown_checks.is_empty() {
            AuditResult::Incomplete
        } else {
            AuditResult::PassWithinScope
        };
    }
}

#[async_trait]
pub trait CkbRpc: Send + Sync {
    async fn get_tip_header(&self) -> Result<Option<HeaderView>>;
    async fn get_header_by_number(&self, number: u64) -> Result<Option<HeaderView>>;
    async fn get_header(&self, hash: &H256) -> Result<Option<HeaderView>>;
    async fn get_block_by_number(&self, number: u64) -> Result<Option<BlockView>>;
    async fn get_transaction(&self, hash: &H256) -> Result<Option<TransactionWithStatusResponse>>;
    async fn get_block_economic_state(&self, hash: &H256) -> Result<Option<BlockEconomicState>>;
    async fn calculate_dao_maximum_withdraw(
        &self,
        out_point: OutPoint,
        withdraw_block_hash: H256,
    ) -> Result<Option<Uint64>>;
}

#[derive(Clone)]
pub struct HttpRpc {
    client: Client,
    url: String,
    max_retries: u32,
}

impl HttpRpc {
    pub fn new(url: String, timeout_secs: u64, max_retries: u32) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .build()
            .context("failed to build http client")?;
        Ok(Self {
            client,
            url,
            max_retries,
        })
    }

    async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T> {
        let mut last_err = None;
        for _ in 0..=self.max_retries {
            let payload = json!({
                "id": 1,
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
            });
            match self.client.post(&self.url).json(&payload).send().await {
                Ok(resp) => {
                    let value: serde_json::Value = resp.json().await.context("invalid rpc json")?;
                    if let Some(err) = value.get("error") {
                        last_err = Some(anyhow!("rpc {} error: {}", method, err));
                        continue;
                    }
                    let result = value
                        .get("result")
                        .ok_or_else(|| anyhow!("rpc {} missing result", method))?
                        .clone();
                    return serde_json::from_value(result).context("rpc result decode failed");
                }
                Err(err) => {
                    last_err = Some(anyhow!("rpc {} request failed: {}", method, err));
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        Err(last_err.unwrap_or_else(|| anyhow!("rpc {} failed", method)))
    }
}

#[async_trait]
impl CkbRpc for HttpRpc {
    async fn get_tip_header(&self) -> Result<Option<HeaderView>> {
        self.call("get_tip_header", json!([])).await
    }

    async fn get_header_by_number(&self, number: u64) -> Result<Option<HeaderView>> {
        self.call("get_header_by_number", json!([format!("0x{number:x}")]))
            .await
    }

    async fn get_header(&self, hash: &H256) -> Result<Option<HeaderView>> {
        self.call("get_header", json!([format!("{hash:#x}")])).await
    }

    async fn get_block_by_number(&self, number: u64) -> Result<Option<BlockView>> {
        self.call("get_block_by_number", json!([format!("0x{number:x}")]))
            .await
    }

    async fn get_transaction(&self, hash: &H256) -> Result<Option<TransactionWithStatusResponse>> {
        self.call(
            "get_transaction",
            json!([format!("{hash:#x}"), {"only_committed": true}]),
        )
        .await
    }

    async fn get_block_economic_state(&self, hash: &H256) -> Result<Option<BlockEconomicState>> {
        self.call("get_block_economic_state", json!([format!("{hash:#x}")]))
            .await
    }

    async fn calculate_dao_maximum_withdraw(
        &self,
        out_point: OutPoint,
        withdraw_block_hash: H256,
    ) -> Result<Option<Uint64>> {
        self.call(
            "calculate_dao_maximum_withdraw",
            json!([out_point, format!("{withdraw_block_hash:#x}")]),
        )
        .await
    }
}

pub enum LogSink {
    Stdout,
    File(PathBuf),
}

impl LogSink {
    pub fn write_json_line(&self, line: &str) -> Result<()> {
        match self {
            LogSink::Stdout => {
                println!("{}", line);
            }
            LogSink::File(path) => {
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .with_context(|| format!("failed to open log file {}", path.display()))?;
                writeln!(file, "{}", line)?;
            }
        }
        Ok(())
    }
}

pub struct Auditor<R: CkbRpc> {
    rpc: Arc<R>,
    config: AuditorConfig,
    sink: LogSink,
}

impl<R: CkbRpc> Auditor<R> {
    pub fn new(rpc: Arc<R>, config: AuditorConfig) -> Self {
        let sink = config
            .log_path
            .as_ref()
            .map(|p| LogSink::File(p.clone()))
            .unwrap_or(LogSink::Stdout);
        Self { rpc, config, sink }
    }

    pub async fn run(&self) -> Result<()> {
        let mut cursor = CursorState::load(&self.config.cursor_path).await?;
        loop {
            if let Err(err) = self.poll_once(&mut cursor).await {
                eprintln!("poll error: {err:#}");
            }
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    eprintln!("shutdown signal received");
                    return Ok(());
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(self.config.poll_interval_ms)) => {}
            }
        }
    }

    async fn poll_once(&self, cursor: &mut Option<CursorState>) -> Result<()> {
        let tip = self
            .rpc
            .get_tip_header()
            .await?
            .ok_or_else(|| anyhow!("tip header missing"))?;

        let tip_height = tip.inner.number.value();
        let tip_hash = format!("{:#x}", tip.hash);

        if cursor.is_none() {
            let mut state = CursorState {
                last_height: tip_height,
                last_hash: tip_hash,
                history: BTreeMap::new(),
            };
            state.push_block(
                tip_height,
                state.last_hash.clone(),
                self.config.history_retention,
            );
            state.save(&self.config.cursor_path).await?;
            *cursor = Some(state);
            eprintln!("first startup anchored at tip height {}", tip_height);
            return Ok(());
        }

        let state = cursor.as_mut().expect("cursor exists");
        let start_height = self.resolve_common_ancestor(state).await? + 1;
        if tip_height < start_height {
            return Ok(());
        }

        for height in start_height..=tip_height {
            let Some(block) = self.rpc.get_block_by_number(height).await? else {
                eprintln!("missing block at height {height}, stop this round");
                break;
            };
            let log = self.audit_block(&block).await;
            let line = serde_json::to_string(&log)?;
            self.sink.write_json_line(&line)?;
            let hash = format!("{:#x}", block.header.hash);
            state.push_block(height, hash, self.config.history_retention);
            state.save(&self.config.cursor_path).await?;
        }

        Ok(())
    }

    async fn resolve_common_ancestor(&self, state: &CursorState) -> Result<u64> {
        let Some(current) = self.rpc.get_header_by_number(state.last_height).await? else {
            return Ok(state.last_height);
        };
        if format!("{:#x}", current.hash) == state.last_hash {
            return Ok(state.last_height);
        }

        eprintln!(
            "reorg detected at height {}, old={}, new={:#x}",
            state.last_height, state.last_hash, current.hash
        );
        let mut heights: Vec<u64> = state.history.keys().copied().collect();
        heights.sort_by(|a, b| b.cmp(a));
        for height in heights {
            let Some(local_hash) = state.history.get(&height) else {
                continue;
            };
            let Some(header) = self.rpc.get_header_by_number(height).await? else {
                continue;
            };
            if format!("{:#x}", header.hash) == *local_hash {
                return Ok(height);
            }
        }

        eprintln!(
            "reorg deeper than retention ({}), anchoring to current tip on next poll",
            self.config.history_retention
        );
        Ok(self
            .rpc
            .get_tip_header()
            .await?
            .map(|h| h.inner.number.value().saturating_sub(1))
            .unwrap_or(state.last_height))
    }

    async fn audit_block(&self, block: &BlockView) -> AuditLog {
        let started = Instant::now();
        let mut log = AuditLog::new(&self.config, block);
        let core_block: CoreBlockView = block.clone().into();

        self.audit_header_and_block(block, &core_block, &mut log)
            .await;
        self.audit_transactions(block, &core_block, &mut log).await;
        self.audit_reward(block, &mut log).await;

        log.audit_duration_ms = started.elapsed().as_millis() as u64;
        log.finalize();
        log
    }

    async fn audit_header_and_block(
        &self,
        block: &BlockView,
        core_block: &CoreBlockView,
        log: &mut AuditLog,
    ) {
        let header_number = block.header.inner.number.value();

        if header_number == 0 {
            log.check_block_height = CheckStatus::Pass;
            log.check_epoch_continuity = CheckStatus::Pass;
            log.check_timestamp = CheckStatus::Pass;
        } else {
            match self.rpc.get_header(&block.header.inner.parent_hash).await {
                Ok(Some(parent)) => {
                    let parent_number = parent.inner.number.value();
                    if header_number == parent_number + 1 {
                        log.check_block_height = CheckStatus::Pass;
                    } else {
                        log.check_block_height = CheckStatus::Fail;
                        log.push_detail(
                            &self.config,
                            DetailItem {
                                check_name: "check_block_height".to_string(),
                                status: CheckStatus::Fail,
                                error_code: "BLOCK_NUMBER_MISMATCH".to_string(),
                                tx_hash: None,
                                tx_index: None,
                                input_index: None,
                                output_index: None,
                                expected_operator: Some("less_than_or_equal".to_string()),
                                expected_value: Some((parent_number + 1).to_string()),
                                actual_value: Some(header_number.to_string()),
                                unit: Some("block".to_string()),
                                reason: "block number must be parent number + 1".to_string(),
                            },
                        );
                    }

                    let parent_core: ckb_types::core::HeaderView = parent.clone().into();
                    let parent_recomputed: H256 = parent_core.data().calc_header_hash().unpack();
                    if parent_recomputed == block.header.inner.parent_hash {
                        log.check_parent_hash = CheckStatus::Pass;
                    } else {
                        log.check_parent_hash = CheckStatus::Fail;
                    }

                    let (ok, reason) = epoch_continuity(
                        parent.inner.epoch.value(),
                        block.header.inner.epoch.value(),
                    );
                    log.check_epoch_continuity = if ok {
                        CheckStatus::Pass
                    } else {
                        CheckStatus::Fail
                    };
                    if !ok {
                        log.push_detail(
                            &self.config,
                            DetailItem {
                                check_name: "check_epoch_continuity".to_string(),
                                status: CheckStatus::Fail,
                                error_code: "EPOCH_CONTINUITY_FAIL".to_string(),
                                tx_hash: None,
                                tx_index: None,
                                input_index: None,
                                output_index: None,
                                expected_operator: None,
                                expected_value: None,
                                actual_value: None,
                                unit: None,
                                reason,
                            },
                        );
                    }

                    let mut timestamps = vec![parent.inner.timestamp.value()];
                    let mut walk_hash = parent.inner.parent_hash;
                    for _ in 0..self.config.median_time_span.saturating_sub(1) {
                        match self.rpc.get_header(&walk_hash).await {
                            Ok(Some(h)) => {
                                timestamps.push(h.inner.timestamp.value());
                                walk_hash = h.inner.parent_hash;
                            }
                            _ => break,
                        }
                    }
                    timestamps.sort_unstable();
                    let median = timestamps[timestamps.len() / 2];
                    let ts = block.header.inner.timestamp.value();
                    let now_ms = Utc::now().timestamp_millis().max(0) as u64;
                    if ts > median && ts <= now_ms.saturating_add(self.config.max_future_ms) {
                        log.check_timestamp = CheckStatus::Pass;
                    } else {
                        log.check_timestamp = CheckStatus::Fail;
                    }
                }
                Ok(None) => {
                    log.check_block_height = CheckStatus::Unknown;
                    log.check_parent_hash = CheckStatus::Unknown;
                    log.check_epoch_continuity = CheckStatus::Unknown;
                    log.check_timestamp = CheckStatus::Unknown;
                }
                Err(err) => {
                    let reason = format!("parent header rpc failed: {err}");
                    for check_name in [
                        "check_block_height",
                        "check_parent_hash",
                        "check_epoch_continuity",
                        "check_timestamp",
                    ] {
                        log.push_detail(
                            &self.config,
                            DetailItem {
                                check_name: check_name.to_string(),
                                status: CheckStatus::Unknown,
                                error_code: "PARENT_HEADER_UNAVAILABLE".to_string(),
                                tx_hash: None,
                                tx_index: None,
                                input_index: None,
                                output_index: None,
                                expected_operator: None,
                                expected_value: None,
                                actual_value: None,
                                unit: None,
                                reason: reason.clone(),
                            },
                        );
                    }
                    log.check_block_height = CheckStatus::Unknown;
                    log.check_parent_hash = CheckStatus::Unknown;
                    log.check_epoch_continuity = CheckStatus::Unknown;
                    log.check_timestamp = CheckStatus::Unknown;
                }
            }
        }

        let block_data: packed::Block = core_block.data();
        let actual_block_size = block_data.serialized_size_without_uncle_proposals();
        log.block_consensus_size_bytes = actual_block_size;
        log.block_consensus_size_limit_bytes = 597_688_320;
        log.check_block_size = if actual_block_size <= log.block_consensus_size_limit_bytes {
            CheckStatus::Pass
        } else {
            CheckStatus::Fail
        };

        log.check_proposal_limit = if block.proposals.len() <= self.config.proposal_limit {
            CheckStatus::Pass
        } else {
            CheckStatus::Fail
        };

        let computed_header_hash: H256 = core_block.data().header().calc_header_hash().unpack();
        log.check_block_hash = if computed_header_hash == block.header.hash {
            CheckStatus::Pass
        } else {
            CheckStatus::Fail
        };

        let computed_tx_hashes = block_data.calc_tx_hashes();
        let tx_hash_status = computed_tx_hashes
            .iter()
            .zip(block.transactions.iter())
            .all(|(computed, tx)| {
                let computed: H256 = computed.unpack();
                computed == tx.hash
            });
        log.check_transaction_hashes = if tx_hash_status {
            CheckStatus::Pass
        } else {
            CheckStatus::Fail
        };

        let computed_transactions_root: H256 = core_block.calc_transactions_root().unpack();
        log.check_transactions_root =
            if computed_transactions_root == block.header.inner.transactions_root {
                CheckStatus::Pass
            } else {
                CheckStatus::Fail
            };

        let computed_proposals_hash: H256 = block_data.calc_proposals_hash().unpack();
        log.check_proposals_hash = if computed_proposals_hash == block.header.inner.proposals_hash {
            CheckStatus::Pass
        } else {
            CheckStatus::Fail
        };

        let computed_extra_hash: H256 = core_block.calc_extra_hash().extra_hash().unpack();
        log.check_extra_hash = if computed_extra_hash == block.header.inner.extra_hash {
            CheckStatus::Pass
        } else {
            CheckStatus::Fail
        };

        let mut tx_hash_set = HashSet::new();
        let mut tx_dup = false;
        for tx in &block.transactions {
            let key = format!("{:#x}", tx.hash);
            if !tx_hash_set.insert(key) {
                tx_dup = true;
                break;
            }
        }
        log.check_duplicate_transactions = if tx_dup {
            CheckStatus::Fail
        } else {
            CheckStatus::Pass
        };

        let mut proposal_set = HashSet::new();
        let mut proposal_dup = false;
        for p in &block.proposals {
            let key = format!("{p:?}");
            if !proposal_set.insert(key) {
                proposal_dup = true;
                break;
            }
        }
        log.check_duplicate_proposals = if proposal_dup {
            CheckStatus::Fail
        } else {
            CheckStatus::Pass
        };

        log.check_cellbase_structure = self.check_cellbase_structure(block);
    }

    fn check_cellbase_structure(&self, block: &BlockView) -> CheckStatus {
        let Some(first_tx) = block.transactions.first() else {
            return CheckStatus::Fail;
        };

        let first_packed: packed::Transaction = first_tx.inner.clone().into();
        if !first_packed.is_cellbase() {
            return CheckStatus::Fail;
        }

        if block.transactions.iter().skip(1).any(|tx| {
            let packed_tx: packed::Transaction = tx.inner.clone().into();
            packed_tx.is_cellbase()
        }) {
            return CheckStatus::Fail;
        }

        if first_tx.inner.outputs.len() > 1
            || first_tx.inner.outputs_data.len() > 1
            || first_tx.inner.outputs.len() != first_tx.inner.outputs_data.len()
        {
            return CheckStatus::Fail;
        }

        if let Some(input) = first_tx.inner.inputs.first() {
            let out_point: packed::OutPoint = input.previous_output.clone().into();
            if !out_point.is_null() {
                return CheckStatus::Fail;
            }
            if input.since.value() != block.header.inner.number.value() {
                return CheckStatus::Fail;
            }
        } else {
            return CheckStatus::Fail;
        }

        if first_tx.inner.witnesses.len() != 1 {
            return CheckStatus::Fail;
        }

        if packed::CellbaseWitness::from_slice(first_tx.inner.witnesses[0].as_bytes()).is_err() {
            return CheckStatus::Fail;
        }

        if let Some(output) = first_tx.inner.outputs.first() {
            if !output.type_.is_none() {
                return CheckStatus::Fail;
            }
            if output.lock.hash_type == ScriptHashType::Data2 {
                return CheckStatus::Fail;
            }
        }

        CheckStatus::Pass
    }

    async fn audit_transactions(
        &self,
        block: &BlockView,
        core_block: &CoreBlockView,
        log: &mut AuditLog,
    ) {
        let mut overall_tx_version = CheckStatus::Pass;
        let mut overall_struct = CheckStatus::Pass;
        let mut overall_data_len = CheckStatus::Pass;
        let mut overall_lock_hash_type = CheckStatus::Pass;
        let mut overall_dup_cell_dep = CheckStatus::Pass;
        let mut overall_dup_header_dep = CheckStatus::Pass;
        let mut overall_dup_input_tx = CheckStatus::Pass;
        let mut overall_dup_input_block = CheckStatus::Pass;
        let mut overall_input_resolution = CheckStatus::Pass;
        let mut overall_input_index = CheckStatus::Pass;
        let mut overall_occupied_capacity = CheckStatus::Pass;
        let mut overall_ordinary_capacity = CheckStatus::Pass;

        let mut input_cache: HashMap<String, Option<(TransactionView, Option<H256>)>> =
            HashMap::new();
        let mut block_inputs = HashSet::new();

        for (tx_index, tx) in block.transactions.iter().enumerate() {
            let packed_tx: packed::Transaction = tx.inner.clone().into();
            if packed_tx.is_cellbase() {
                continue;
            }

            log.capacity_eligible_tx_count += 1;

            if tx.inner.version.value() != self.config.tx_version {
                overall_tx_version = merge_status(overall_tx_version, CheckStatus::Fail);
            }

            if tx.inner.inputs.is_empty() || tx.inner.outputs.is_empty() {
                overall_struct = merge_status(overall_struct, CheckStatus::Fail);
            }

            if tx.inner.outputs.len() != tx.inner.outputs_data.len() {
                overall_data_len = merge_status(overall_data_len, CheckStatus::Fail);
            }

            if tx
                .inner
                .outputs
                .iter()
                .any(|o| matches!(o.lock.hash_type, ScriptHashType::Data2))
            {
                overall_lock_hash_type = merge_status(overall_lock_hash_type, CheckStatus::Fail);
            }

            let mut cell_dep_set = HashSet::new();
            if tx.inner.cell_deps.iter().any(|dep| {
                let key = format!(
                    "{:#x}:{}:{:?}",
                    dep.out_point.tx_hash,
                    dep.out_point.index.value(),
                    dep.dep_type
                );
                !cell_dep_set.insert(key)
            }) {
                overall_dup_cell_dep = merge_status(overall_dup_cell_dep, CheckStatus::Fail);
            }

            let mut header_dep_set = HashSet::new();
            if tx
                .inner
                .header_deps
                .iter()
                .any(|h| !header_dep_set.insert(format!("{:#x}", h)))
            {
                overall_dup_header_dep = merge_status(overall_dup_header_dep, CheckStatus::Fail);
            }

            let mut input_set = HashSet::new();
            if tx.inner.inputs.iter().any(|input| {
                let key = format!(
                    "{:#x}:{}",
                    input.previous_output.tx_hash,
                    input.previous_output.index.value()
                );
                !input_set.insert(key)
            }) {
                overall_dup_input_tx = merge_status(overall_dup_input_tx, CheckStatus::Fail);
            }

            if tx.inner.inputs.iter().any(|input| {
                let key = format!(
                    "{:#x}:{}",
                    input.previous_output.tx_hash,
                    input.previous_output.index.value()
                );
                !block_inputs.insert(key)
            }) {
                overall_dup_input_block = merge_status(overall_dup_input_block, CheckStatus::Fail);
            }

            let mut ordinary_input_sum: u128 = 0;
            let mut ordinary_output_sum: u128 = 0;
            let mut tx_unknown = false;
            let mut has_dao_input = false;
            let mut dao_effective_sum: u128 = 0;

            for (output_index, output) in tx.inner.outputs.iter().enumerate() {
                ordinary_output_sum =
                    match ordinary_output_sum.checked_add(output.capacity.value() as u128) {
                        Some(v) => v,
                        None => {
                            overall_ordinary_capacity =
                                merge_status(overall_ordinary_capacity, CheckStatus::Fail);
                            tx_unknown = true;
                            0
                        }
                    };
                let data_capacity = tx
                    .inner
                    .outputs_data
                    .get(output_index)
                    .and_then(|data| OccupiedCapacity::bytes(data.len()).ok())
                    .unwrap_or_else(OccupiedCapacity::zero);
                if packed::CellOutput::from(output.clone())
                    .is_lack_of_capacity(data_capacity)
                    .unwrap_or(true)
                {
                    overall_occupied_capacity =
                        merge_status(overall_occupied_capacity, CheckStatus::Fail);
                }
            }

            for (input_index, input) in tx.inner.inputs.iter().enumerate() {
                let input_tx_hash = input.previous_output.tx_hash.clone();
                let key = format!("{:#x}", input_tx_hash);

                if !input_cache.contains_key(&key) {
                    let fetched = match self.rpc.get_transaction(&input_tx_hash).await {
                        Ok(Some(resp)) => {
                            let tx_opt =
                                resp.transaction
                                    .and_then(|r: ResponseFormat<TransactionView>| match r.inner {
                                        Either::Left(v) => Some(v),
                                        Either::Right(_) => None,
                                    });
                            let block_hash = resp.tx_status.block_hash;
                            tx_opt.map(|t| (t, block_hash))
                        }
                        _ => None,
                    };
                    input_cache.insert(key.clone(), fetched);
                }

                let Some((input_tx, input_tx_block_hash)) =
                    input_cache.get(&key).cloned().flatten()
                else {
                    tx_unknown = true;
                    log.unresolved_input_count += 1;
                    overall_input_resolution =
                        merge_status(overall_input_resolution, CheckStatus::Unknown);
                    continue;
                };

                if input_tx.hash != input_tx_hash {
                    overall_input_resolution =
                        merge_status(overall_input_resolution, CheckStatus::Fail);
                    continue;
                }

                let idx = input.previous_output.index.value() as usize;
                if idx >= input_tx.inner.outputs.len() {
                    tx_unknown = true;
                    overall_input_index = merge_status(overall_input_index, CheckStatus::Fail);
                    continue;
                }

                let resolved_output = &input_tx.inner.outputs[idx];
                let is_dao_input = resolved_output.type_.as_ref().is_some_and(|script| {
                    script.hash_type == ScriptHashType::Type
                        && format!("{:#x}", script.code_hash)
                            .eq_ignore_ascii_case(&self.config.dao_type_hash)
                });

                if is_dao_input {
                    has_dao_input = true;
                    if let Some(_prev_block_hash) = input_tx_block_hash {
                        match self
                            .rpc
                            .calculate_dao_maximum_withdraw(
                                input.previous_output.clone(),
                                block.header.hash.clone(),
                            )
                            .await
                        {
                            Ok(Some(v)) => {
                                dao_effective_sum =
                                    dao_effective_sum.saturating_add(v.value() as u128);
                                log.dao_checked_inputs_count += 1;
                            }
                            Ok(None) | Err(_) => {
                                tx_unknown = true;
                                log.dao_unresolved_inputs_count += 1;
                            }
                        }
                    } else {
                        tx_unknown = true;
                        log.dao_unresolved_inputs_count += 1;
                    }
                } else {
                    ordinary_input_sum =
                        ordinary_input_sum.saturating_add(resolved_output.capacity.value() as u128);
                }

                if tx_unknown {
                    log.push_detail(
                        &self.config,
                        DetailItem {
                            check_name: "check_input_content_resolution".to_string(),
                            status: CheckStatus::Unknown,
                            error_code: "INPUT_UNRESOLVED".to_string(),
                            tx_hash: Some(format!("{:#x}", tx.hash)),
                            tx_index: Some(tx_index),
                            input_index: Some(input_index),
                            output_index: None,
                            expected_operator: None,
                            expected_value: None,
                            actual_value: None,
                            unit: None,
                            reason: "input dependency unavailable from rpc".to_string(),
                        },
                    );
                }
            }

            if has_dao_input {
                log.dao_related_tx_count += 1;
                let expected = ordinary_input_sum.saturating_add(dao_effective_sum);
                if ordinary_output_sum > expected {
                    log.check_dao_withdraw_capacity = CheckStatus::Fail;
                    log.push_detail(
                        &self.config,
                        DetailItem {
                            check_name: "check_dao_withdraw_capacity".to_string(),
                            status: CheckStatus::Fail,
                            error_code: "DAO_WITHDRAW_CAPACITY_EXCEEDED".to_string(),
                            tx_hash: Some(format!("{:#x}", tx.hash)),
                            tx_index: Some(tx_index),
                            input_index: None,
                            output_index: None,
                            expected_operator: Some("less_than_or_equal".to_string()),
                            expected_value: Some(expected.to_string()),
                            actual_value: Some(ordinary_output_sum.to_string()),
                            unit: Some("shannon".to_string()),
                            reason: "dao effective capacity + ordinary inputs must cover outputs"
                                .to_string(),
                        },
                    );
                } else if tx_unknown {
                    if log.check_dao_withdraw_capacity != CheckStatus::Fail {
                        log.check_dao_withdraw_capacity = CheckStatus::Unknown;
                    }
                } else if log.check_dao_withdraw_capacity != CheckStatus::Fail {
                    log.check_dao_withdraw_capacity = CheckStatus::Pass;
                }
                log.capacity_checked_tx_count += 1;
            } else {
                if tx_unknown {
                    overall_ordinary_capacity =
                        merge_status(overall_ordinary_capacity, CheckStatus::Unknown);
                    log.capacity_unchecked_tx_count += 1;
                } else if ordinary_output_sum > ordinary_input_sum {
                    overall_ordinary_capacity =
                        merge_status(overall_ordinary_capacity, CheckStatus::Fail);
                    log.capacity_failed_tx_count += 1;
                    log.capacity_checked_tx_count += 1;
                    log.push_detail(
                        &self.config,
                        DetailItem {
                            check_name: "check_ordinary_capacity_conservation".to_string(),
                            status: CheckStatus::Fail,
                            error_code: "OUTPUT_CAPACITY_EXCEEDS_INPUT".to_string(),
                            tx_hash: Some(format!("{:#x}", tx.hash)),
                            tx_index: Some(tx_index),
                            input_index: None,
                            output_index: None,
                            expected_operator: Some("less_than_or_equal".to_string()),
                            expected_value: Some(ordinary_input_sum.to_string()),
                            actual_value: Some(ordinary_output_sum.to_string()),
                            unit: Some("shannon".to_string()),
                            reason: "ordinary outputs must be <= ordinary inputs".to_string(),
                        },
                    );
                } else {
                    log.capacity_checked_tx_count += 1;
                }
            }
        }

        if log.dao_related_tx_count == 0 {
            log.check_dao_withdraw_capacity = CheckStatus::NotApplicable;
        }

        let _ = core_block;
        log.check_transaction_version = overall_tx_version;
        log.check_inputs_outputs_structure = overall_struct;
        log.check_outputs_data_length = overall_data_len;
        log.check_output_lock_hash_type = overall_lock_hash_type;
        log.check_duplicate_cell_deps = overall_dup_cell_dep;
        log.check_duplicate_header_deps = overall_dup_header_dep;
        log.check_duplicate_inputs_in_transaction = overall_dup_input_tx;
        log.check_duplicate_inputs_in_block = overall_dup_input_block;
        log.check_input_content_resolution = overall_input_resolution;
        log.check_input_output_index = overall_input_index;
        log.check_occupied_capacity = overall_occupied_capacity;
        log.check_ordinary_capacity_conservation = overall_ordinary_capacity;
    }

    async fn audit_reward(&self, block: &BlockView, log: &mut AuditLog) {
        let block_hash = block.header.hash.clone();
        let Some(cellbase) = block.transactions.first() else {
            log.check_cellbase_reward_amount = CheckStatus::Fail;
            log.check_cellbase_reward_target = CheckStatus::Unknown;
            return;
        };

        let actual = cellbase.inner.outputs.iter().fold(0u128, |acc, o| {
            acc.saturating_add(o.capacity.value() as u128)
        });
        log.reward_actual_amount = Some(actual.to_string());

        match self.rpc.get_block_economic_state(&block_hash).await {
            Ok(Some(economic)) => {
                let expected = economic.miner_reward.primary.value() as u128
                    + economic.miner_reward.secondary.value() as u128
                    + economic.miner_reward.committed.value() as u128
                    + economic.miner_reward.proposal.value() as u128;
                log.reward_expected_amount = Some(expected.to_string());
                log.reward_target_block_hash = Some(format!("{:#x}", economic.finalized_at));

                if actual <= expected
                    || (cellbase.inner.outputs.is_empty()
                        && expected.saturating_sub(actual) < 61 * 100_000_000u128)
                {
                    log.check_cellbase_reward_amount = CheckStatus::Pass;
                } else {
                    log.check_cellbase_reward_amount = CheckStatus::Fail;
                    log.push_detail(
                        &self.config,
                        DetailItem {
                            check_name: "check_cellbase_reward_amount".to_string(),
                            status: CheckStatus::Fail,
                            error_code: "CELLBASE_REWARD_MISMATCH".to_string(),
                            tx_hash: Some(format!("{:#x}", cellbase.hash)),
                            tx_index: Some(0),
                            input_index: None,
                            output_index: None,
                            expected_operator: Some("equal".to_string()),
                            expected_value: Some(expected.to_string()),
                            actual_value: Some(actual.to_string()),
                            unit: Some("shannon".to_string()),
                            reason: "cellbase output total does not match rpc economic state"
                                .to_string(),
                        },
                    );
                }

                if expected == 0 {
                    log.check_cellbase_reward_target = CheckStatus::Unknown;
                } else {
                    let lock = cellbase.inner.witnesses.first().and_then(|w| {
                        packed::CellbaseWitness::from_slice(w.as_bytes())
                            .ok()
                            .map(|x| x.lock())
                    });
                    if let Some(lock) = lock {
                        let expected_lock = ckb_jsonrpc_types::Script::from(lock);
                        if cellbase
                            .inner
                            .outputs
                            .iter()
                            .all(|o| o.lock == expected_lock)
                        {
                            log.check_cellbase_reward_target = CheckStatus::Pass;
                        } else {
                            log.check_cellbase_reward_target = CheckStatus::Fail;
                        }
                    } else {
                        log.check_cellbase_reward_target = CheckStatus::Unknown;
                    }
                }
            }
            Ok(None) => {
                log.check_cellbase_reward_amount = CheckStatus::Unknown;
                log.check_cellbase_reward_target = CheckStatus::Unknown;
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_cellbase_reward_amount".to_string(),
                        status: CheckStatus::Unknown,
                        error_code: "ECONOMIC_STATE_MISSING".to_string(),
                        tx_hash: None,
                        tx_index: None,
                        input_index: None,
                        output_index: None,
                        expected_operator: None,
                        expected_value: None,
                        actual_value: None,
                        unit: None,
                        reason: "get_block_economic_state returned null".to_string(),
                    },
                );
            }
            Err(err) => {
                log.check_cellbase_reward_amount = CheckStatus::Unknown;
                log.check_cellbase_reward_target = CheckStatus::Unknown;
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_cellbase_reward_amount".to_string(),
                        status: CheckStatus::Unknown,
                        error_code: "ECONOMIC_STATE_RPC_ERROR".to_string(),
                        tx_hash: None,
                        tx_index: None,
                        input_index: None,
                        output_index: None,
                        expected_operator: None,
                        expected_value: None,
                        actual_value: None,
                        unit: None,
                        reason: format!("get_block_economic_state failed: {err}"),
                    },
                );
            }
        }
    }
}

fn merge_status(old: CheckStatus, new_status: CheckStatus) -> CheckStatus {
    use CheckStatus::*;
    let rank = |s| match s {
        Fail => 4,
        Unknown => 3,
        Pass => 2,
        NotApplicable => 1,
        NotImplemented => 0,
    };
    if rank(new_status) > rank(old) {
        new_status
    } else {
        old
    }
}

fn epoch_continuity(parent_value: u64, current_value: u64) -> (bool, String) {
    let parent = EpochNumberWithFraction::from_full_value(parent_value);
    let current = EpochNumberWithFraction::from_full_value(current_value);

    if parent.number() == current.number()
        && current.index() == parent.index() + 1
        && current.length() == parent.length()
    {
        return (true, String::new());
    }

    if current.number() == parent.number() + 1 && current.index() == 0 {
        return (true, String::new());
    }

    (
        false,
        format!(
            "invalid epoch continuity parent={}:{}:{} current={}:{}:{}",
            parent.number(),
            parent.index(),
            parent.length(),
            current.number(),
            current.index(),
            current.length()
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ckb_jsonrpc_types::{Capacity, TxStatus};
    use ckb_types::core::{BlockBuilder, HeaderBuilder, TransactionBuilder};
    use ckb_types::packed::{Byte32, CellInput, CellOutput, OutPoint as PackedOutPoint, ScriptOpt};
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockRpc {
        tip: Mutex<Option<HeaderView>>,
        headers_by_number: Mutex<HashMap<u64, HeaderView>>,
        headers_by_hash: Mutex<HashMap<String, HeaderView>>,
        blocks_by_number: Mutex<HashMap<u64, BlockView>>,
        txs: Mutex<HashMap<String, serde_json::Value>>,
        economics: Mutex<HashMap<String, BlockEconomicState>>,
        dao_max: Mutex<HashMap<String, Uint64>>,
    }

    #[async_trait]
    impl CkbRpc for MockRpc {
        async fn get_tip_header(&self) -> Result<Option<HeaderView>> {
            Ok(self.tip.lock().unwrap().clone())
        }
        async fn get_header_by_number(&self, number: u64) -> Result<Option<HeaderView>> {
            Ok(self.headers_by_number.lock().unwrap().get(&number).cloned())
        }
        async fn get_header(&self, hash: &H256) -> Result<Option<HeaderView>> {
            Ok(self
                .headers_by_hash
                .lock()
                .unwrap()
                .get(&format!("{hash:#x}"))
                .cloned())
        }
        async fn get_block_by_number(&self, number: u64) -> Result<Option<BlockView>> {
            Ok(self.blocks_by_number.lock().unwrap().get(&number).cloned())
        }
        async fn get_transaction(
            &self,
            hash: &H256,
        ) -> Result<Option<TransactionWithStatusResponse>> {
            Ok(self
                .txs
                .lock()
                .unwrap()
                .get(&format!("{hash:#x}"))
                .cloned()
                .map(serde_json::from_value)
                .transpose()?)
        }
        async fn get_block_economic_state(
            &self,
            hash: &H256,
        ) -> Result<Option<BlockEconomicState>> {
            Ok(self
                .economics
                .lock()
                .unwrap()
                .get(&format!("{hash:#x}"))
                .cloned())
        }
        async fn calculate_dao_maximum_withdraw(
            &self,
            out_point: OutPoint,
            _withdraw_block_hash: H256,
        ) -> Result<Option<Uint64>> {
            Ok(self
                .dao_max
                .lock()
                .unwrap()
                .get(&format!(
                    "{:#x}:{}",
                    out_point.tx_hash,
                    out_point.index.value()
                ))
                .cloned())
        }
    }

    #[test]
    fn test_epoch_continuity() {
        let parent = EpochNumberWithFraction::new(10, 1, 100).full_value();
        let current = EpochNumberWithFraction::new(10, 2, 100).full_value();
        assert!(epoch_continuity(parent, current).0);

        let bad = EpochNumberWithFraction::new(12, 0, 100).full_value();
        assert!(!epoch_continuity(parent, bad).0);
    }

    #[tokio::test]
    async fn test_cursor_load_save() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cursor.json");
        let c = CursorState {
            last_height: 1,
            last_hash: "0x1".to_string(),
            history: BTreeMap::from([(1, "0x1".to_string())]),
        };
        c.save(&path).await.unwrap();
        let loaded = CursorState::load(&path).await.unwrap().unwrap();
        assert_eq!(loaded.last_height, 1);
        assert_eq!(loaded.last_hash, "0x1");
    }

    fn test_config(cursor_path: PathBuf) -> AuditorConfig {
        AuditorConfig {
            rpc_url: "http://127.0.0.1:8114".to_string(),
            network: "testnet".to_string(),
            node_id: "node-1".to_string(),
            poll_interval_ms: 1000,
            rpc_timeout_secs: 10,
            max_retries: 0,
            cursor_path,
            log_path: None,
            max_details: 100,
            max_future_ms: 15_000,
            median_time_span: 11,
            proposal_limit: 1500,
            tx_version: 0,
            history_retention: 32,
            dao_type_hash: "0x82d76d1b75c9f0c49f1437f446f40c6f735af754987eb07f76f4c2f2f5f6f2b2"
                .to_string(),
        }
    }

    fn simple_lock_script() -> ckb_types::packed::Script {
        ckb_types::packed::Script::new_builder()
            .code_hash(Byte32::zero())
            .hash_type(ckb_types::core::ScriptHashType::Data)
            .args(ckb_types::bytes::Bytes::new())
            .build()
    }

    fn simple_cell_output(cap: u64) -> CellOutput {
        CellOutput::new_builder()
            .capacity(cap)
            .lock(simple_lock_script())
            .type_(ScriptOpt::default())
            .build()
    }

    #[tokio::test]
    async fn test_first_startup_anchor_tip_only() {
        let dir = tempfile::tempdir().unwrap();
        let cursor_path = dir.path().join("cursor.json");
        let cfg = test_config(cursor_path.clone());

        let tip_header = HeaderBuilder::default()
            .number(100u64)
            .timestamp(1000u64)
            .epoch(EpochNumberWithFraction::new(0, 100, 1000).full_value())
            .build();
        let tip_json: HeaderView = tip_header.into();

        let rpc = Arc::new(MockRpc::default());
        *rpc.tip.lock().unwrap() = Some(tip_json);

        let auditor = Auditor::new(rpc, cfg);
        let mut cursor = None;
        auditor.poll_once(&mut cursor).await.unwrap();

        let loaded = CursorState::load(&cursor_path).await.unwrap().unwrap();
        assert_eq!(loaded.last_height, 100);
    }

    #[tokio::test]
    async fn test_ordinary_capacity_fail() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path().join("cursor.json"));

        let parent = HeaderBuilder::default()
            .number(1u64)
            .timestamp(1000u64)
            .epoch(EpochNumberWithFraction::new(0, 0, 1000).full_value())
            .build();

        let cellbase = TransactionBuilder::default()
            .version(0u32)
            .input(CellInput::new_cellbase_input(2))
            .output(simple_cell_output(500_00000000))
            .output_data(ckb_types::bytes::Bytes::new())
            .witness(simple_lock_script().into_witness())
            .build();

        let prev_tx = TransactionBuilder::default()
            .version(0u32)
            .output(simple_cell_output(100))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();

        let prev_out_point = PackedOutPoint::new(prev_tx.hash(), 0);
        let spend_tx = TransactionBuilder::default()
            .version(0u32)
            .input(CellInput::new(prev_out_point.clone(), 0))
            .output(simple_cell_output(110))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();

        let block = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(2u64)
                    .timestamp(1200u64)
                    .epoch(EpochNumberWithFraction::new(0, 1, 1000).full_value())
                    .parent_hash(parent.hash())
                    .build(),
            )
            .transaction(cellbase.clone())
            .transaction(spend_tx.clone())
            .build();

        let block_json: BlockView = block.clone().into();
        let parent_json: HeaderView = parent.clone().into();
        let prev_tx_json: TransactionView = prev_tx.clone().into();

        let tx_resp = TransactionWithStatusResponse {
            transaction: Some(ResponseFormat::json(prev_tx_json)),
            cycles: None,
            time_added_to_pool: None,
            tx_status: TxStatus::committed(1u64.into(), parent.hash().unpack(), 0u32.into()),
            fee: None,
            min_replace_fee: None,
        };

        let economic = BlockEconomicState {
            issuance: Default::default(),
            miner_reward: ckb_jsonrpc_types::MinerReward {
                primary: Capacity::from(500_00000000u64),
                secondary: Capacity::from(0u64),
                committed: Capacity::from(0u64),
                proposal: Capacity::from(0u64),
            },
            txs_fee: Capacity::from(0u64),
            finalized_at: parent.hash().unpack(),
        };

        let rpc = Arc::new(MockRpc::default());
        rpc.headers_by_hash.lock().unwrap().insert(
            format!("{:#x}", block_json.header.inner.parent_hash),
            parent_json.clone(),
        );
        rpc.txs.lock().unwrap().insert(
            format!("{:#x}", prev_tx.hash()),
            serde_json::to_value(tx_resp).unwrap(),
        );
        rpc.economics
            .lock()
            .unwrap()
            .insert(format!("{:#x}", block.hash()), economic);

        let auditor = Auditor::new(rpc, cfg);
        let log = auditor.audit_block(&block_json).await;
        assert_eq!(log.check_ordinary_capacity_conservation, CheckStatus::Fail);
        assert_eq!(log.result, AuditResult::Fail);
        assert!(!log.failed_checks.is_empty());
    }
}
