use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::error::Error as StdError;
use std::fmt;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use ckb_jsonrpc_types::{
    BlockEconomicState, BlockView, Consensus as RpcConsensus, DaoWithdrawingCalculationKind,
    Either, HeaderView, OutPoint, ResponseFormat, ScriptHashType, Status, TransactionView,
    TransactionWithStatusResponse, Uint64,
};
use ckb_occupied_capacity::Capacity as OccupiedCapacity;
use ckb_types::H256;
use ckb_types::core::{BlockView as CoreBlockView, EpochNumberWithFraction};
use ckb_types::packed;
use ckb_types::prelude::*;
use reqwest::header::RETRY_AFTER;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

const RETRY_BASE_DELAY_MS: u64 = 100;
const RATE_LIMIT_FALLBACK_DELAY_SECS: u64 = 60;
const DEFAULT_RPC_MIN_INTERVAL_MS: u64 = 100;
const DEFAULT_RPC_MAX_INTERVAL_MS: u64 = 2000;
const DEFAULT_RPC_MAX_CONCURRENCY: usize = 2;
const RATE_INTERVAL_INCREASE_FACTOR: u64 = 2;
const RATE_RECOVERY_SUCCESS_WINDOW: u64 = 20;
const PENDING_LOG_REMINDER_ROUNDS: u32 = 10;
const PENDING_EXECUTION_BACKOFF_FLOOR_SECS: u64 = 15;
const PENDING_EXECUTION_BACKOFF_CAP_SECS: u64 = 300;

#[derive(Debug, Clone)]
pub struct AuditorConfig {
    pub rpc_url: String,
    pub node_id: String,
    pub poll_interval_ms: u64,
    pub rpc_timeout_secs: u64,
    pub max_retries: u32,
    pub cursor_path: Option<PathBuf>,
    pub log_path: Option<PathBuf>,
    pub max_details: usize,
    pub max_future_ms: u64,
    pub median_time_span: usize,
    pub proposal_limit: usize,
    pub tx_version: u32,
    pub history_retention: usize,
    pub dao_type_hash: String,
    pub header_cache_capacity: usize,
    pub block_cache_entries: usize,
    pub block_cache_max_bytes: usize,
    pub stats_interval_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub genesis_hash: Option<String>,
    pub last_height: u64,
    pub last_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_height: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_hash: Option<String>,
    #[serde(default)]
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
        let temp_name = format!(
            ".{}.tmp-{}",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("cursor"),
            std::process::id()
        );
        let temp_path = path.with_file_name(temp_name);
        tokio::fs::write(&temp_path, serde_json::to_vec_pretty(self)?)
            .await
            .with_context(|| format!("failed to write temp cursor file {}", temp_path.display()))?;
        tokio::fs::rename(&temp_path, path)
            .await
            .with_context(|| format!("failed to replace cursor file {}", path.display()))
    }

    fn new(genesis_hash: String, height: u64, hash: String, retention: usize) -> Self {
        let mut state = Self {
            genesis_hash: Some(genesis_hash),
            last_height: height,
            last_hash: hash.clone(),
            next_height: None,
            next_hash: None,
            history: BTreeMap::new(),
        };
        state.push_block(height, hash, retention);
        state
    }

    fn new_pending(genesis_hash: String, block: &BlockView, retention: usize) -> Self {
        let height = block.header.inner.number.value();
        let hash = format!("{:#x}", block.header.hash);
        let mut state = Self {
            genesis_hash: Some(genesis_hash),
            last_height: height.saturating_sub(1),
            last_hash: if height == 0 {
                String::new()
            } else {
                format!("{:#x}", block.header.inner.parent_hash)
            },
            next_height: None,
            next_hash: None,
            history: BTreeMap::new(),
        };
        if height > 0 {
            state.push_block(
                height - 1,
                format!("{:#x}", block.header.inner.parent_hash),
                retention,
            );
        }
        state.next_height = Some(height);
        state.next_hash = Some(hash);
        state
    }

    fn push_block(&mut self, height: u64, hash: String, retention: usize) {
        self.last_height = height;
        self.last_hash = hash.clone();
        self.next_height = None;
        self.next_hash = None;
        self.history.insert(height, hash);
        while self.history.len() > retention {
            if let Some(key) = self.history.keys().next().copied() {
                self.history.remove(&key);
            }
        }
    }

    fn mark_pending(&mut self, block: &BlockView, retention: usize) {
        let height = block.header.inner.number.value();
        self.next_height = Some(height);
        self.next_hash = Some(format!("{:#x}", block.header.hash));
        if height > 0 {
            self.last_height = height - 1;
            self.last_hash = format!("{:#x}", block.header.inner.parent_hash);
            self.history
                .insert(self.last_height, self.last_hash.clone());
            while self.history.len() > retention {
                if let Some(key) = self.history.keys().next().copied() {
                    self.history.remove(&key);
                }
            }
        }
    }
}

#[derive(Debug)]
struct ShutdownError {
    context: &'static str,
}

impl fmt::Display for ShutdownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "shutdown signal received {}", self.context)
    }
}

impl StdError for ShutdownError {}

fn shutdown_error(context: &'static str) -> anyhow::Error {
    anyhow!(ShutdownError { context })
}

fn is_shutdown_error(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|cause| cause.downcast_ref::<ShutdownError>().is_some())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EmittedAuditKey {
    block_hash: String,
    result: AuditResult,
}

#[derive(Debug, Default)]
struct EmittedAuditWindow {
    keys: VecDeque<EmittedAuditKey>,
    limit: usize,
}

impl EmittedAuditWindow {
    fn load(path: Option<&Path>, limit: usize) -> Self {
        let mut window = Self {
            keys: VecDeque::new(),
            limit: limit.max(1),
        };
        let Some(path) = path else {
            return window;
        };
        let Ok(content) = std::fs::read_to_string(path) else {
            return window;
        };
        let recent_lines: Vec<_> = content.lines().rev().take(window.limit).collect();
        for line in recent_lines.into_iter().rev() {
            let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            let Some(block_hash) = value.get("block_hash").and_then(|item| item.as_str()) else {
                continue;
            };
            let Some(result) = value.get("result").and_then(|item| item.as_str()) else {
                continue;
            };
            window.remember(EmittedAuditKey {
                block_hash: block_hash.to_string(),
                result: match result {
                    "PASS" => AuditResult::Pass,
                    "FAIL" => AuditResult::Fail,
                    _ => continue,
                },
            });
        }
        window
    }

    fn contains(&self, key: &EmittedAuditKey) -> bool {
        self.keys.iter().any(|existing| existing == key)
    }

    fn remember(&mut self, key: EmittedAuditKey) {
        if self.contains(&key) {
            return;
        }
        self.keys.push_back(key);
        while self.keys.len() > self.limit {
            self.keys.pop_front();
        }
    }
}

#[derive(Debug, Default)]
struct PendingAuditWindow {
    entries: HashMap<String, PendingAuditState>,
    order: VecDeque<String>,
    limit: usize,
}

#[derive(Debug, Clone)]
struct PendingAuditState {
    fingerprint: String,
    first_seen: std::time::Duration,
    last_logged: std::time::Duration,
    rounds: u32,
    repeats_since_change: u32,
    next_retry_not_before: Option<std::time::Duration>,
}

#[derive(Debug, Clone)]
struct PendingAuditObservation {
    rounds: u32,
    repeats_since_change: u32,
    first_seen: std::time::Duration,
    should_log: bool,
    reason_changed: bool,
}

#[derive(Debug, Clone)]
struct PendingAuditRecovery {
    rounds: u32,
    first_seen: std::time::Duration,
}

impl PendingAuditWindow {
    fn new(limit: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            limit: limit.max(1),
        }
    }

    fn remember_key(&mut self, block_hash: &str) {
        self.order.retain(|existing| existing != block_hash);
        self.order.push_back(block_hash.to_string());
        while self.order.len() > self.limit {
            if let Some(evicted) = self.order.pop_front() {
                self.entries.remove(&evicted);
            }
        }
    }

    fn next_retry_not_before(
        &self,
        block_hash: &str,
        now: std::time::Duration,
    ) -> Option<std::time::Duration> {
        self.entries
            .get(block_hash)
            .and_then(|state| state.next_retry_not_before)
            .filter(|deadline| *deadline > now)
    }

    fn next_round(&self, block_hash: &str) -> u32 {
        self.entries
            .get(block_hash)
            .map(|state| state.rounds.saturating_add(1))
            .unwrap_or(1)
    }

    fn observe_pending(
        &mut self,
        now: std::time::Duration,
        block_hash: &str,
        fingerprint: String,
        next_retry_not_before: Option<std::time::Duration>,
    ) -> PendingAuditObservation {
        self.remember_key(block_hash);
        let state = self
            .entries
            .entry(block_hash.to_string())
            .or_insert_with(|| PendingAuditState {
                fingerprint: fingerprint.clone(),
                first_seen: now,
                last_logged: now,
                rounds: 0,
                repeats_since_change: 0,
                next_retry_not_before,
            });
        state.rounds = state.rounds.saturating_add(1);
        state.next_retry_not_before = next_retry_not_before;

        let reason_changed = state.fingerprint != fingerprint;
        if reason_changed {
            state.fingerprint = fingerprint;
            state.repeats_since_change = 0;
            state.last_logged = now;
            return PendingAuditObservation {
                rounds: state.rounds,
                repeats_since_change: 0,
                first_seen: state.first_seen,
                should_log: true,
                reason_changed: true,
            };
        }

        let first_round = state.rounds == 1;
        if !first_round {
            state.repeats_since_change = state.repeats_since_change.saturating_add(1);
        }
        let should_log = first_round
            || state
                .repeats_since_change
                .is_multiple_of(PENDING_LOG_REMINDER_ROUNDS);
        if should_log {
            state.last_logged = now;
        }
        PendingAuditObservation {
            rounds: state.rounds,
            repeats_since_change: state.repeats_since_change,
            first_seen: state.first_seen,
            should_log,
            reason_changed: false,
        }
    }

    fn resolve(&mut self, block_hash: &str) -> Option<PendingAuditRecovery> {
        self.order.retain(|existing| existing != block_hash);
        self.entries
            .remove(block_hash)
            .map(|state| PendingAuditRecovery {
                rounds: state.rounds,
                first_seen: state.first_seen,
            })
    }
}

#[derive(Debug, Default, Clone)]
struct RpcCooldownState {
    deadline: Option<std::time::Duration>,
    resume_at_utc: Option<DateTime<Utc>>,
    method: Option<String>,
    reason: Option<String>,
    delay_source: Option<&'static str>,
    wait_duration: Option<std::time::Duration>,
    awaiting_recovery: Option<CooldownRecoveryState>,
}

#[derive(Debug, Clone)]
struct RpcPacingState {
    base_interval: std::time::Duration,
    max_interval: std::time::Duration,
    current_interval: std::time::Duration,
    next_send_at: std::time::Duration,
    success_since_adjustment: u64,
}

#[derive(Debug, Default, Clone)]
struct RpcMetricsState {
    total_http_attempts: u64,
    total_429_responses: u64,
    method_attempts: HashMap<String, u64>,
    cooldown_wait_ms: u64,
    rate_gate_wait_ms: u64,
}

enum HeightAuditOutcome {
    Finalized {
        block: BlockView,
        log: AuditLog,
    },
    Pending {
        block: BlockView,
        log: AuditLog,
        block_attempt: u32,
        total_block_attempts: u32,
        non_retryable_execution: bool,
    },
}

#[derive(Debug, Clone)]
struct CooldownRecoveryState {
    method: String,
    reason: String,
    resume_at_utc: DateTime<Utc>,
    delay_source: &'static str,
    wait_duration: std::time::Duration,
}

#[derive(Debug, Clone, Copy)]
struct RetryDelayDecision {
    delay: std::time::Duration,
    source: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub struct HttpRpcPacingConfig {
    pub min_interval_ms: u64,
    pub max_interval_ms: u64,
    pub max_concurrency: usize,
}

impl Default for HttpRpcPacingConfig {
    fn default() -> Self {
        Self {
            min_interval_ms: DEFAULT_RPC_MIN_INTERVAL_MS,
            max_interval_ms: DEFAULT_RPC_MAX_INTERVAL_MS,
            max_concurrency: DEFAULT_RPC_MAX_CONCURRENCY,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RpcMetricsSnapshot {
    pub total_http_attempts: u64,
    pub total_429_responses: u64,
    pub method_attempts: HashMap<String, u64>,
    pub cooldown_wait_ms: u64,
    pub rate_gate_wait_ms: u64,
    pub current_min_interval_ms: u64,
}

#[async_trait]
trait RpcClock: Send + Sync {
    fn now(&self) -> std::time::Duration;
    fn now_utc(&self) -> DateTime<Utc>;
    async fn sleep(
        &self,
        delay: std::time::Duration,
        shutdown: &CancellationToken,
        context: &'static str,
    ) -> Result<()>;
}

struct SystemRpcClock {
    started_at: std::time::Instant,
}

impl SystemRpcClock {
    fn new() -> Self {
        Self {
            started_at: std::time::Instant::now(),
        }
    }
}

#[async_trait]
impl RpcClock for SystemRpcClock {
    fn now(&self) -> std::time::Duration {
        self.started_at.elapsed()
    }

    fn now_utc(&self) -> DateTime<Utc> {
        Utc::now()
    }

    async fn sleep(
        &self,
        delay: std::time::Duration,
        shutdown: &CancellationToken,
        context: &'static str,
    ) -> Result<()> {
        tokio::select! {
            _ = tokio::time::sleep(delay) => Ok(()),
            _ = shutdown.cancelled() => Err(shutdown_error(context)),
            _ = tokio::signal::ctrl_c() => {
                shutdown.cancel();
                Err(shutdown_error(context))
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
}

impl CheckStatus {
    fn is_omitted(&self) -> bool {
        matches!(self, Self::NotApplicable)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AuditResult {
    Fail,
    Pass,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FailureKind {
    ValidationFailed,
    RetryExhausted,
    ExecutionFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailItem {
    pub check_name: String,
    pub status: CheckStatus,
    pub error_code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_kind: Option<FailureKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempts: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub referenced_out_point: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_operator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone)]
struct ConsensusSnapshot {
    consensus_id: String,
    genesis_hash: H256,
    dao_type_hash: H256,
    max_block_bytes: usize,
    max_uncles_num: usize,
    proposal_limit: usize,
    block_version: u32,
    tx_version: u32,
    median_time_block_count: usize,
    finalization_delay_length: u64,
}

impl ConsensusSnapshot {
    fn from_rpc(config: &AuditorConfig, consensus: RpcConsensus) -> Result<Self> {
        if !config.dao_type_hash.is_empty()
            && !format!("{:#x}", consensus.dao_type_hash)
                .eq_ignore_ascii_case(&config.dao_type_hash)
        {
            return Err(anyhow!(
                "configured dao type hash '{}' does not match node consensus dao type hash '{:#x}'",
                config.dao_type_hash,
                consensus.dao_type_hash
            ));
        }

        Ok(Self {
            consensus_id: consensus.id.clone(),
            genesis_hash: consensus.genesis_hash,
            dao_type_hash: consensus.dao_type_hash,
            max_block_bytes: usize::try_from(consensus.max_block_bytes.value())
                .context("consensus max_block_bytes exceeds platform usize")?,
            max_uncles_num: usize::try_from(consensus.max_uncles_num.value())
                .context("consensus max_uncles_num exceeds platform usize")?,
            proposal_limit: usize::try_from(consensus.max_block_proposals_limit.value())
                .context("consensus max_block_proposals_limit exceeds platform usize")?,
            block_version: consensus.block_version.value(),
            tx_version: consensus.tx_version.value(),
            median_time_block_count: usize::try_from(consensus.median_time_block_count.value())
                .context("consensus median_time_block_count exceeds platform usize")?,
            finalization_delay_length: consensus.tx_proposal_window.farthest.value() + 1,
        })
    }
}

#[derive(Debug, Clone)]
struct ResolvedCommittedTransaction {
    tx: TransactionView,
    block_hash: H256,
    block_number: Option<u64>,
}

#[derive(Debug, Clone)]
struct ResolutionIssue {
    status: CheckStatus,
    error_code: String,
    reason: String,
}

#[derive(Debug, Clone)]
enum CachedTransaction {
    Resolved(ResolvedCommittedTransaction),
    Unavailable(ResolutionIssue),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLog {
    pub timestamp: String,
    pub schema_version: u32,
    pub service: String,
    pub auditor_version: String,
    pub node_id: String,

    pub block_height: u64,
    pub block_hash: String,
    pub parent_hash: String,
    pub block_timestamp: u64,
    pub canonical_at_audit: bool,

    pub result: AuditResult,
    pub audit_duration_ms: u64,

    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_block_height: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_parent_hash: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_epoch_continuity: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_timestamp: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_block_version: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_block_size: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_uncle_count_limit: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_proposal_limit: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_block_hash: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_transaction_hashes: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_transactions_root: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_proposals_hash: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_extra_hash: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_duplicate_transactions: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_duplicate_proposals: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_cellbase_structure: CheckStatus,

    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_transaction_version: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_inputs_outputs_structure: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_outputs_data_length: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_output_lock_hash_type: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_duplicate_cell_deps: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_duplicate_header_deps: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_duplicate_inputs_in_transaction: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_duplicate_inputs_in_block: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_input_content_resolution: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_input_output_index: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_occupied_capacity: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_ordinary_capacity_conservation: CheckStatus,

    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_cellbase_reward_amount: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_cellbase_reward_target: CheckStatus,
    #[serde(skip_serializing_if = "CheckStatus::is_omitted")]
    pub check_dao_withdraw_capacity: CheckStatus,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_checks: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details_truncated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details_total: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Vec<DetailItem>>,
}

impl AuditLog {
    fn new(config: &AuditorConfig, block: &BlockView) -> Self {
        Self {
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            schema_version: 4,
            service: "ckb-block-auditor".to_string(),
            auditor_version: env!("CARGO_PKG_VERSION").to_string(),
            node_id: config.node_id.clone(),
            block_height: block.header.inner.number.value(),
            block_hash: format!("{:#x}", block.header.hash),
            parent_hash: format!("{:#x}", block.header.inner.parent_hash),
            block_timestamp: block.header.inner.timestamp.value(),
            canonical_at_audit: true,
            result: AuditResult::Fail,
            audit_duration_ms: 0,

            check_block_height: CheckStatus::Unknown,
            check_parent_hash: CheckStatus::Unknown,
            check_epoch_continuity: CheckStatus::Unknown,
            check_timestamp: CheckStatus::Unknown,
            check_block_version: CheckStatus::Unknown,
            check_block_size: CheckStatus::Unknown,
            check_uncle_count_limit: CheckStatus::Unknown,
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
            check_output_lock_hash_type: CheckStatus::NotApplicable,
            check_duplicate_cell_deps: CheckStatus::Unknown,
            check_duplicate_header_deps: CheckStatus::Unknown,
            check_duplicate_inputs_in_transaction: CheckStatus::Unknown,
            check_duplicate_inputs_in_block: CheckStatus::Unknown,
            check_input_content_resolution: CheckStatus::NotApplicable,
            check_input_output_index: CheckStatus::NotApplicable,
            check_occupied_capacity: CheckStatus::NotApplicable,
            check_ordinary_capacity_conservation: CheckStatus::NotApplicable,

            check_cellbase_reward_amount: CheckStatus::Unknown,
            check_cellbase_reward_target: CheckStatus::Unknown,
            check_dao_withdraw_capacity: CheckStatus::NotApplicable,

            failed_checks: None,
            details_truncated: None,
            details_total: None,
            details: None,
        }
    }

    fn push_detail(&mut self, config: &AuditorConfig, item: DetailItem) {
        let total = self.details_total.get_or_insert(0);
        *total += 1;
        let details = self.details.get_or_insert_with(Vec::new);
        if details.len() < config.max_details {
            details.push(item);
        } else {
            self.details_truncated = Some(true);
        }
    }

    fn finalize(&mut self, attempts: u32, max_retries: u32) {
        let checks = vec![
            ("check_block_height", self.check_block_height),
            ("check_parent_hash", self.check_parent_hash),
            ("check_epoch_continuity", self.check_epoch_continuity),
            ("check_timestamp", self.check_timestamp),
            ("check_block_version", self.check_block_version),
            ("check_block_size", self.check_block_size),
            ("check_uncle_count_limit", self.check_uncle_count_limit),
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

        let mut failed_checks: Vec<String> = checks
            .iter()
            .filter_map(|(name, status)| {
                (*status == CheckStatus::Fail).then_some((*name).to_string())
            })
            .collect();
        let unresolved_checks: Vec<String> = checks
            .iter()
            .filter_map(|(name, status)| {
                (*status == CheckStatus::Unknown).then_some((*name).to_string())
            })
            .collect();

        failed_checks.extend(unresolved_checks.iter().cloned());
        self.failed_checks = (!failed_checks.is_empty()).then_some(failed_checks);
        if self.details_total.unwrap_or(0) > 0 {
            self.details_truncated.get_or_insert(false);
            self.details.get_or_insert_with(Vec::new);
        } else {
            self.details = None;
            self.details_total = None;
            self.details_truncated = None;
        }

        if let Some(details) = self.details.as_mut() {
            for detail in details {
                match detail.status {
                    CheckStatus::Fail => {
                        detail
                            .failure_kind
                            .get_or_insert(FailureKind::ValidationFailed);
                    }
                    CheckStatus::Unknown => {
                        detail.status = CheckStatus::Fail;
                        detail.failure_kind =
                            Some(if is_retryable_execution_error(&detail.error_code) {
                                FailureKind::RetryExhausted
                            } else {
                                FailureKind::ExecutionFailed
                            });
                        if detail.rpc_method.is_none() {
                            detail.rpc_method =
                                rpc_method_for_error_code(&detail.error_code).map(str::to_string);
                        }
                        detail.attempts = Some(attempts);
                        detail.max_retries = Some(max_retries);
                    }
                    _ => {}
                }
            }
        }

        self.map_unknown_checks_to_fail();
        self.result = if self.failed_checks.is_some() {
            AuditResult::Fail
        } else {
            AuditResult::Pass
        };
    }

    fn has_detail_for(&self, check_name: &str) -> bool {
        self.details
            .as_ref()
            .is_some_and(|details| details.iter().any(|detail| detail.check_name == check_name))
    }

    fn has_unresolved_checks(&self) -> bool {
        self.check_statuses()
            .iter()
            .any(|(_, status)| *status == CheckStatus::Unknown)
    }

    fn has_non_retryable_execution_failure(&self) -> bool {
        self.details.as_ref().is_some_and(|details| {
            details.iter().any(|detail| {
                detail.status == CheckStatus::Unknown
                    && !is_block_retryable_execution_error(&detail.error_code)
            })
        })
    }

    fn check_statuses(&self) -> Vec<(&'static str, CheckStatus)> {
        vec![
            ("check_block_height", self.check_block_height),
            ("check_parent_hash", self.check_parent_hash),
            ("check_epoch_continuity", self.check_epoch_continuity),
            ("check_timestamp", self.check_timestamp),
            ("check_block_version", self.check_block_version),
            ("check_block_size", self.check_block_size),
            ("check_uncle_count_limit", self.check_uncle_count_limit),
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
        ]
    }

    fn map_unknown_checks_to_fail(&mut self) {
        for status in [
            &mut self.check_block_height,
            &mut self.check_parent_hash,
            &mut self.check_epoch_continuity,
            &mut self.check_timestamp,
            &mut self.check_block_version,
            &mut self.check_block_size,
            &mut self.check_uncle_count_limit,
            &mut self.check_proposal_limit,
            &mut self.check_block_hash,
            &mut self.check_transaction_hashes,
            &mut self.check_transactions_root,
            &mut self.check_proposals_hash,
            &mut self.check_extra_hash,
            &mut self.check_duplicate_transactions,
            &mut self.check_duplicate_proposals,
            &mut self.check_cellbase_structure,
            &mut self.check_transaction_version,
            &mut self.check_inputs_outputs_structure,
            &mut self.check_outputs_data_length,
            &mut self.check_output_lock_hash_type,
            &mut self.check_duplicate_cell_deps,
            &mut self.check_duplicate_header_deps,
            &mut self.check_duplicate_inputs_in_transaction,
            &mut self.check_duplicate_inputs_in_block,
            &mut self.check_input_content_resolution,
            &mut self.check_input_output_index,
            &mut self.check_occupied_capacity,
            &mut self.check_ordinary_capacity_conservation,
            &mut self.check_cellbase_reward_amount,
            &mut self.check_cellbase_reward_target,
            &mut self.check_dao_withdraw_capacity,
        ] {
            if *status == CheckStatus::Unknown {
                *status = CheckStatus::Fail;
            }
        }
    }
}

#[async_trait]
pub trait CkbRpc: Send + Sync {
    async fn get_tip_header(&self) -> Result<Option<HeaderView>>;
    async fn get_block(&self, hash: &H256) -> Result<Option<BlockView>>;
    async fn get_header_by_number(&self, number: u64) -> Result<Option<HeaderView>>;
    async fn get_header(&self, hash: &H256) -> Result<Option<HeaderView>>;
    async fn get_block_by_number(&self, number: u64) -> Result<Option<BlockView>>;
    async fn get_transaction(&self, hash: &H256) -> Result<Option<TransactionWithStatusResponse>>;
    async fn get_block_economic_state(&self, hash: &H256) -> Result<Option<BlockEconomicState>>;
    async fn get_consensus(&self) -> Result<RpcConsensus>;
    async fn calculate_dao_maximum_withdraw(
        &self,
        out_point: OutPoint,
        kind: DaoWithdrawingCalculationKind,
    ) -> Result<Option<Uint64>>;

    fn has_rate_limit_cooldown(&self) -> bool {
        false
    }

    fn metrics_snapshot(&self) -> Option<RpcMetricsSnapshot> {
        None
    }
}

#[derive(Clone)]
pub struct HttpRpc {
    client: Client,
    url: String,
    max_retries: u32,
    cooldown: Arc<StdMutex<RpcCooldownState>>,
    pacing: Arc<StdMutex<RpcPacingState>>,
    semaphore: Arc<Semaphore>,
    metrics: Arc<StdMutex<RpcMetricsState>>,
    shutdown: CancellationToken,
    clock: Arc<dyn RpcClock>,
}

impl HttpRpc {
    pub fn new(url: String, timeout_secs: u64, max_retries: u32) -> Result<Self> {
        Self::new_with_pacing(
            url,
            timeout_secs,
            max_retries,
            HttpRpcPacingConfig::default(),
        )
    }

    pub fn new_with_pacing(
        url: String,
        timeout_secs: u64,
        max_retries: u32,
        pacing: HttpRpcPacingConfig,
    ) -> Result<Self> {
        Self::new_with_dependencies(
            url,
            timeout_secs,
            max_retries,
            pacing,
            CancellationToken::new(),
            Arc::new(SystemRpcClock::new()),
        )
    }

    #[cfg(test)]
    fn new_for_test(
        url: String,
        timeout_secs: u64,
        max_retries: u32,
        shutdown: CancellationToken,
        clock: Arc<dyn RpcClock>,
    ) -> Result<Self> {
        Self::new_with_dependencies(
            url,
            timeout_secs,
            max_retries,
            HttpRpcPacingConfig::default(),
            shutdown,
            clock,
        )
    }

    fn new_with_dependencies(
        url: String,
        timeout_secs: u64,
        max_retries: u32,
        pacing: HttpRpcPacingConfig,
        shutdown: CancellationToken,
        clock: Arc<dyn RpcClock>,
    ) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .build()
            .context("failed to build http client")?;
        let min_interval_ms = pacing.min_interval_ms.max(1);
        let max_interval_ms = pacing.max_interval_ms.max(min_interval_ms);
        let base_interval = std::time::Duration::from_millis(min_interval_ms);
        let max_interval = std::time::Duration::from_millis(max_interval_ms);
        Ok(Self {
            client,
            url,
            max_retries,
            cooldown: Arc::new(StdMutex::new(RpcCooldownState::default())),
            pacing: Arc::new(StdMutex::new(RpcPacingState {
                base_interval,
                max_interval,
                current_interval: base_interval,
                next_send_at: std::time::Duration::ZERO,
                success_since_adjustment: 0,
            })),
            semaphore: Arc::new(Semaphore::new(pacing.max_concurrency.max(1))),
            metrics: Arc::new(StdMutex::new(RpcMetricsState::default())),
            shutdown,
            clock,
        })
    }

    fn current_cooldown_deadline(&self) -> Option<std::time::Duration> {
        self.cooldown
            .lock()
            .unwrap()
            .deadline
            .filter(|deadline| *deadline > self.clock.now())
    }

    async fn wait_for_cooldown(&self) -> Result<()> {
        loop {
            let deadline = {
                let mut state = self.cooldown.lock().unwrap();
                match state.deadline {
                    Some(deadline) if deadline > self.clock.now() => Some(deadline),
                    Some(_) => {
                        let resume_at = state.resume_at_utc.take().unwrap_or_else(Utc::now);
                        let method = state.method.take().unwrap_or_else(|| "unknown".to_string());
                        let reason = state.reason.take().unwrap_or_default();
                        let delay_source = state.delay_source.take().unwrap_or("fallback");
                        let wait_duration = state.wait_duration.take().unwrap_or_default();
                        state.deadline = None;
                        eprintln!(
                            "rpc cooldown expired/resuming trigger_method={} delay_source={} wait_ms={} next_try_at={}{}",
                            method,
                            delay_source,
                            wait_duration.as_millis(),
                            resume_at.to_rfc3339_opts(SecondsFormat::Millis, true),
                            format_reason_suffix(&reason)
                        );
                        state.awaiting_recovery = Some(CooldownRecoveryState {
                            method,
                            reason,
                            resume_at_utc: resume_at,
                            delay_source,
                            wait_duration,
                        });
                        None
                    }
                    None => None,
                }
            };
            let Some(deadline) = deadline else {
                return Ok(());
            };
            let wait = deadline.saturating_sub(self.clock.now());
            {
                let mut metrics = self.metrics.lock().unwrap();
                metrics.cooldown_wait_ms = metrics
                    .cooldown_wait_ms
                    .saturating_add(wait.as_millis() as u64);
            }
            self.sleep(wait, "during rpc cooldown").await?;
        }
    }

    async fn acquire_rate_permit(&self) -> Result<OwnedSemaphorePermit> {
        tokio::select! {
            permit = self.semaphore.clone().acquire_owned() => {
                permit.context("rpc request semaphore closed")
            }
            _ = self.shutdown.cancelled() => Err(shutdown_error("while waiting for rpc concurrency permit")),
        }
    }

    async fn wait_for_rate_slot(&self) -> Result<()> {
        loop {
            let delay = {
                let mut pacing = self.pacing.lock().unwrap();
                let now = self.clock.now();
                let start_at = pacing.next_send_at.max(now);
                let delay = start_at.saturating_sub(now);
                if delay.is_zero() {
                    pacing.next_send_at = now + pacing.current_interval;
                }
                delay
            };
            if delay.is_zero() {
                return Ok(());
            }
            {
                let mut metrics = self.metrics.lock().unwrap();
                metrics.rate_gate_wait_ms = metrics
                    .rate_gate_wait_ms
                    .saturating_add(delay.as_millis() as u64);
            }
            self.sleep(delay, "during rpc rate pacing").await?;
        }
    }

    fn on_rate_limited(&self) {
        let mut pacing = self.pacing.lock().unwrap();
        pacing.success_since_adjustment = 0;
        let mut next = pacing
            .current_interval
            .saturating_mul(RATE_INTERVAL_INCREASE_FACTOR as u32);
        if next > pacing.max_interval {
            next = pacing.max_interval;
        }
        pacing.current_interval = next.max(pacing.base_interval);
    }

    fn on_success(&self) {
        let mut pacing = self.pacing.lock().unwrap();
        pacing.success_since_adjustment = pacing.success_since_adjustment.saturating_add(1);
        if pacing.success_since_adjustment < RATE_RECOVERY_SUCCESS_WINDOW {
            return;
        }
        pacing.success_since_adjustment = 0;
        if pacing.current_interval <= pacing.base_interval {
            pacing.current_interval = pacing.base_interval;
            return;
        }
        let diff = pacing.current_interval.saturating_sub(pacing.base_interval);
        let reduction = std::time::Duration::from_millis((diff.as_millis() as u64 / 4).max(1));
        pacing.current_interval = pacing.current_interval.saturating_sub(reduction);
        if pacing.current_interval < pacing.base_interval {
            pacing.current_interval = pacing.base_interval;
        }
    }

    async fn sleep(&self, delay: std::time::Duration, context: &'static str) -> Result<()> {
        self.clock.sleep(delay, &self.shutdown, context).await
    }

    fn parse_retry_after_delay(
        &self,
        retry_after: Option<&reqwest::header::HeaderValue>,
        body: &str,
    ) -> RetryDelayDecision {
        if let Some(header_delay) = retry_after
            .and_then(|value| value.to_str().ok())
            .and_then(|value| parse_retry_after_value(value, self.clock.now_utc()))
            .filter(|delay| !delay.is_zero())
        {
            return RetryDelayDecision {
                delay: header_delay,
                source: "retry_after_header",
            };
        }
        if let Some(body_delay) = parse_retry_after_from_body(body).filter(|delay| !delay.is_zero())
        {
            return RetryDelayDecision {
                delay: body_delay,
                source: "response_body",
            };
        }
        RetryDelayDecision {
            delay: std::time::Duration::from_secs(RATE_LIMIT_FALLBACK_DELAY_SECS),
            source: "fallback",
        }
    }

    fn record_cooldown(&self, method: &str, decision: RetryDelayDecision, reason: &str) {
        self.on_rate_limited();
        {
            let mut metrics = self.metrics.lock().unwrap();
            metrics.total_429_responses = metrics.total_429_responses.saturating_add(1);
        }
        let now = self.clock.now();
        let new_deadline = now + decision.delay;
        let resume_at_utc = chrono::Duration::from_std(decision.delay)
            .ok()
            .map(|delta| self.clock.now_utc() + delta)
            .unwrap_or_else(|| self.clock.now_utc());
        let mut state = self.cooldown.lock().unwrap();
        let prior_deadline = state.deadline.filter(|deadline| *deadline > now);
        state.awaiting_recovery = None;
        let should_extend = prior_deadline.is_some_and(|deadline| new_deadline > deadline);
        if prior_deadline.is_none() {
            eprintln!(
                "rpc cooldown entered method={} http_status=429 delay_source={} wait_ms={} next_try_at={}{}",
                method,
                decision.source,
                decision.delay.as_millis(),
                resume_at_utc.to_rfc3339_opts(SecondsFormat::Millis, true),
                format_reason_suffix(&sanitize_rate_limit_reason(reason))
            );
        } else if should_extend {
            eprintln!(
                "rpc cooldown extended method={} http_status=429 delay_source={} wait_ms={} next_try_at={}{}",
                method,
                decision.source,
                decision.delay.as_millis(),
                resume_at_utc.to_rfc3339_opts(SecondsFormat::Millis, true),
                format_reason_suffix(&sanitize_rate_limit_reason(reason))
            );
        }
        if prior_deadline.is_none() || should_extend {
            state.deadline = Some(new_deadline);
            state.resume_at_utc = Some(resume_at_utc);
            state.method = Some(method.to_string());
            state.reason = Some(sanitize_rate_limit_reason(reason));
            state.delay_source = Some(decision.source);
            state.wait_duration = Some(decision.delay);
        }
    }

    fn mark_cooldown_recovered(&self, success_method: &str) {
        let recovered = self.cooldown.lock().unwrap().awaiting_recovery.take();
        if let Some(recovered) = recovered {
            eprintln!(
                "rpc cooldown recovered after success_method={} trigger_method={} delay_source={} wait_ms={} next_try_at={}{}",
                success_method,
                recovered.method,
                recovered.delay_source,
                recovered.wait_duration.as_millis(),
                recovered
                    .resume_at_utc
                    .to_rfc3339_opts(SecondsFormat::Millis, true),
                format_reason_suffix(&recovered.reason)
            );
        }
    }

    async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T> {
        let mut last_err = None;
        let total_attempts = self.max_retries.saturating_add(1);
        for attempt in 1..=total_attempts {
            self.wait_for_cooldown().await?;
            let _permit = self.acquire_rate_permit().await?;
            self.wait_for_rate_slot().await?;
            self.wait_for_cooldown().await?;
            {
                let mut metrics = self.metrics.lock().unwrap();
                metrics.total_http_attempts = metrics.total_http_attempts.saturating_add(1);
                let entry = metrics
                    .method_attempts
                    .entry(method.to_string())
                    .or_default();
                *entry = entry.saturating_add(1);
            }
            let payload = json!({
                "id": 1,
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
            });
            match self.client.post(&self.url).json(&payload).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if !status.is_success() {
                        let retry_after = resp.headers().get(RETRY_AFTER).cloned();
                        let body = resp.text().await.unwrap_or_default();
                        if status == StatusCode::TOO_MANY_REQUESTS {
                            let decision =
                                self.parse_retry_after_delay(retry_after.as_ref(), &body);
                            self.record_cooldown(method, decision, &body);
                        }
                        last_err = Some(anyhow!(format_http_status_error(
                            method,
                            status,
                            body.trim(),
                            attempt
                        )));
                        if attempt < total_attempts {
                            if status == StatusCode::TOO_MANY_REQUESTS {
                                continue;
                            }
                            self.sleep(retry_backoff(attempt), "during rpc retry backoff")
                                .await?;
                        }
                        continue;
                    }
                    let value: serde_json::Value = resp
                        .json()
                        .await
                        .with_context(|| format!("rpc {method} invalid json"))?;
                    if let Some(err) = value.get("error") {
                        last_err = Some(anyhow!("rpc {} error: {}", method, err));
                        if attempt < total_attempts {
                            self.sleep(retry_backoff(attempt), "during rpc retry backoff")
                                .await?;
                        }
                        continue;
                    }
                    let result = value
                        .get("result")
                        .ok_or_else(|| anyhow!("rpc {} missing result", method))?
                        .clone();
                    self.on_success();
                    self.mark_cooldown_recovered(method);
                    return serde_json::from_value(result)
                        .with_context(|| format!("rpc {method} result decode failed"));
                }
                Err(err) => {
                    last_err = Some(anyhow!(
                        "rpc {} request failed after {} http attempt(s): {}",
                        method,
                        attempt,
                        err.without_url()
                    ));
                }
            }
            if attempt < total_attempts {
                self.sleep(retry_backoff(attempt), "during rpc retry backoff")
                    .await?;
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("rpc {} failed", method)))
    }
}

#[async_trait]
impl CkbRpc for HttpRpc {
    async fn get_tip_header(&self) -> Result<Option<HeaderView>> {
        self.call("get_tip_header", json!([])).await
    }

    async fn get_block(&self, hash: &H256) -> Result<Option<BlockView>> {
        self.call("get_block", json!([format!("{hash:#x}"), "0x2", false]))
            .await
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
            json!([format!("{hash:#x}"), "0x2", true]),
        )
        .await
    }

    async fn get_block_economic_state(&self, hash: &H256) -> Result<Option<BlockEconomicState>> {
        self.call("get_block_economic_state", json!([format!("{hash:#x}")]))
            .await
    }

    async fn get_consensus(&self) -> Result<RpcConsensus> {
        self.call("get_consensus", json!([])).await
    }

    async fn calculate_dao_maximum_withdraw(
        &self,
        out_point: OutPoint,
        kind: DaoWithdrawingCalculationKind,
    ) -> Result<Option<Uint64>> {
        self.call("calculate_dao_maximum_withdraw", json!([out_point, kind]))
            .await
    }

    fn has_rate_limit_cooldown(&self) -> bool {
        self.current_cooldown_deadline().is_some()
    }

    fn metrics_snapshot(&self) -> Option<RpcMetricsSnapshot> {
        let metrics = self.metrics.lock().unwrap().clone();
        let pacing = self.pacing.lock().unwrap().clone();
        Some(RpcMetricsSnapshot {
            total_http_attempts: metrics.total_http_attempts,
            total_429_responses: metrics.total_429_responses,
            method_attempts: metrics.method_attempts,
            cooldown_wait_ms: metrics.cooldown_wait_ms,
            rate_gate_wait_ms: metrics.rate_gate_wait_ms,
            current_min_interval_ms: pacing.current_interval.as_millis() as u64,
        })
    }
}

#[derive(Debug, Default, Clone)]
struct HeaderCacheState {
    map: HashMap<String, HeaderView>,
    order: VecDeque<String>,
    capacity: usize,
    hits: u64,
    misses: u64,
}

#[derive(Debug, Clone)]
struct BlockCacheEntry {
    block: BlockView,
    bytes: usize,
}

#[derive(Debug, Default, Clone)]
struct BlockCacheState {
    map: HashMap<String, BlockCacheEntry>,
    order: VecDeque<String>,
    max_entries: usize,
    max_bytes: usize,
    used_bytes: usize,
    hits: u64,
    misses: u64,
}

#[derive(Debug, Default, Clone)]
struct AuditorStatsState {
    last_report_at: std::time::Duration,
    last_reported_attempts: u64,
    last_completed_height: Option<u64>,
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
    consensus_cache: tokio::sync::Mutex<Option<ConsensusSnapshot>>,
    emitted_audits: StdMutex<EmittedAuditWindow>,
    pending_audits: StdMutex<PendingAuditWindow>,
    header_cache: StdMutex<HeaderCacheState>,
    block_cache: StdMutex<BlockCacheState>,
    stats: StdMutex<AuditorStatsState>,
    shutdown: CancellationToken,
    clock: Arc<dyn RpcClock>,
}

impl<R: CkbRpc> Auditor<R> {
    pub fn new(rpc: Arc<R>, config: AuditorConfig) -> Self {
        Self::new_with_dependencies(
            rpc,
            config,
            CancellationToken::new(),
            Arc::new(SystemRpcClock::new()),
        )
    }

    fn new_with_dependencies(
        rpc: Arc<R>,
        config: AuditorConfig,
        shutdown: CancellationToken,
        clock: Arc<dyn RpcClock>,
    ) -> Self {
        let sink = config
            .log_path
            .as_ref()
            .map(|p| LogSink::File(p.clone()))
            .unwrap_or(LogSink::Stdout);
        let emitted_audits =
            EmittedAuditWindow::load(config.log_path.as_deref(), config.history_retention);
        let pending_audits = PendingAuditWindow::new(config.history_retention);
        let header_cache = HeaderCacheState {
            capacity: config.header_cache_capacity.max(1),
            ..Default::default()
        };
        let block_cache = BlockCacheState {
            max_entries: config.block_cache_entries.max(1),
            max_bytes: config.block_cache_max_bytes.max(1),
            ..Default::default()
        };
        let now = clock.now();
        Self {
            rpc,
            config,
            sink,
            consensus_cache: tokio::sync::Mutex::new(None),
            emitted_audits: StdMutex::new(emitted_audits),
            pending_audits: StdMutex::new(pending_audits),
            header_cache: StdMutex::new(header_cache),
            block_cache: StdMutex::new(block_cache),
            stats: StdMutex::new(AuditorStatsState {
                last_report_at: now,
                ..Default::default()
            }),
            shutdown,
            clock,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut cursor = self.load_cursor().await?;
        loop {
            if let Err(err) = self.poll_once(&mut cursor).await {
                if is_shutdown_error(&err) {
                    eprintln!("shutdown signal received");
                    return Ok(());
                }
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

    async fn load_cursor(&self) -> Result<Option<CursorState>> {
        match &self.config.cursor_path {
            Some(path) => CursorState::load(path).await,
            None => Ok(None),
        }
    }

    async fn save_cursor(&self, state: &CursorState) -> Result<()> {
        if let Some(path) = &self.config.cursor_path {
            state.save(path).await?;
        }
        Ok(())
    }

    fn estimate_block_bytes(block: &BlockView) -> usize {
        serde_json::to_vec(block)
            .map(|bytes| bytes.len())
            .unwrap_or(0)
    }

    fn cache_header(&self, header: &HeaderView) {
        let key = format!("{:#x}", header.hash);
        let mut cache = self.header_cache.lock().unwrap();
        if cache.map.contains_key(&key) {
            cache.order.retain(|existing| existing != &key);
        }
        cache.map.insert(key.clone(), header.clone());
        cache.order.push_back(key);
        while cache.map.len() > cache.capacity {
            if let Some(oldest) = cache.order.pop_front() {
                cache.map.remove(&oldest);
            }
        }
    }

    fn cache_block(&self, block: &BlockView) {
        self.cache_header(&block.header);
        let key = format!("{:#x}", block.header.hash);
        let bytes = Self::estimate_block_bytes(block);
        let mut cache = self.block_cache.lock().unwrap();
        if bytes == 0 || bytes > cache.max_bytes {
            return;
        }
        if let Some(old) = cache.map.remove(&key) {
            cache.used_bytes = cache.used_bytes.saturating_sub(old.bytes);
            cache.order.retain(|existing| existing != &key);
        }
        cache.used_bytes = cache.used_bytes.saturating_add(bytes);
        cache.order.push_back(key.clone());
        cache.map.insert(
            key,
            BlockCacheEntry {
                block: block.clone(),
                bytes,
            },
        );
        while cache.map.len() > cache.max_entries || cache.used_bytes > cache.max_bytes {
            let Some(oldest) = cache.order.pop_front() else {
                break;
            };
            if let Some(evicted) = cache.map.remove(&oldest) {
                cache.used_bytes = cache.used_bytes.saturating_sub(evicted.bytes);
            }
        }
    }

    async fn get_header_cached(&self, hash: &H256) -> Result<Option<HeaderView>> {
        let key = format!("{hash:#x}");
        if let Some(hit) = {
            let mut cache = self.header_cache.lock().unwrap();
            let hit = cache.map.get(&key).cloned();
            if hit.is_some() {
                cache.hits = cache.hits.saturating_add(1);
            } else {
                cache.misses = cache.misses.saturating_add(1);
            }
            hit
        } {
            return Ok(Some(hit));
        }
        let fetched = self.rpc.get_header(hash).await?;
        if let Some(header) = fetched {
            if header.hash != *hash {
                return Err(anyhow!(
                    "get_header returned mismatched hash, requested {hash:#x}, got {:#x}",
                    header.hash
                ));
            }
            self.cache_header(&header);
            return Ok(Some(header));
        }
        Ok(None)
    }

    async fn get_block_cached(&self, hash: &H256) -> Result<Option<BlockView>> {
        let key = format!("{hash:#x}");
        if let Some(hit) = {
            let mut cache = self.block_cache.lock().unwrap();
            let hit = cache.map.get(&key).map(|entry| entry.block.clone());
            if hit.is_some() {
                cache.hits = cache.hits.saturating_add(1);
            } else {
                cache.misses = cache.misses.saturating_add(1);
            }
            hit
        } {
            return Ok(Some(hit));
        }
        let fetched = self.rpc.get_block(hash).await?;
        if let Some(block) = fetched {
            if block.header.hash == *hash {
                self.cache_block(&block);
            }
            return Ok(Some(block));
        }
        Ok(None)
    }

    fn maybe_report_stats(&self, tip_height: u64, current_height: Option<u64>) {
        let interval = std::time::Duration::from_secs(self.config.stats_interval_secs.max(1));
        let now = self.clock.now();
        let mut state = self.stats.lock().unwrap();
        if now.saturating_sub(state.last_report_at) < interval {
            if let Some(height) = current_height {
                state.last_completed_height = Some(height);
            }
            return;
        }
        let rpc_metrics = self.rpc.metrics_snapshot().unwrap_or_default();
        let header_cache = self.header_cache.lock().unwrap().clone();
        let block_cache = self.block_cache.lock().unwrap().clone();
        let elapsed = now.saturating_sub(state.last_report_at).as_secs_f64();
        let current = current_height
            .or(state.last_completed_height)
            .unwrap_or(tip_height);
        let backlog = tip_height.saturating_sub(current);
        let block_rate = state
            .last_completed_height
            .map(|last| current.saturating_sub(last) as f64 / elapsed.max(1e-9))
            .unwrap_or(0.0);
        let attempt_delta = rpc_metrics
            .total_http_attempts
            .saturating_sub(state.last_reported_attempts);
        let req_rate = attempt_delta as f64 / elapsed.max(1e-9);
        eprintln!(
            "operational stats height={} tip={} backlog={} blocks_per_sec={:.3} http_attempts_total={} http_attempts_delta={} req_per_sec={:.3} method_attempts={:?} http_429_total={} cooldown_wait_ms_total={} rate_wait_ms_total={} rate_min_interval_ms={} header_cache_hits={} header_cache_misses={} block_cache_hits={} block_cache_misses={}",
            current,
            tip_height,
            backlog,
            block_rate,
            rpc_metrics.total_http_attempts,
            attempt_delta,
            req_rate,
            rpc_metrics.method_attempts,
            rpc_metrics.total_429_responses,
            rpc_metrics.cooldown_wait_ms,
            rpc_metrics.rate_gate_wait_ms,
            rpc_metrics.current_min_interval_ms,
            header_cache.hits,
            header_cache.misses,
            block_cache.hits,
            block_cache.misses,
        );
        state.last_report_at = now;
        state.last_reported_attempts = rpc_metrics.total_http_attempts;
        if let Some(height) = current_height {
            state.last_completed_height = Some(height);
        }
    }

    fn write_final_audit_once(&self, log: &AuditLog) -> Result<bool> {
        let key = EmittedAuditKey {
            block_hash: log.block_hash.clone(),
            result: log.result,
        };
        let mut emitted = self.emitted_audits.lock().unwrap();
        if emitted.contains(&key) {
            eprintln!(
                "skip duplicate finalized audit output for height {} block {} result {:?}",
                log.block_height, log.block_hash, log.result
            );
            return Ok(false);
        }
        let line = serde_json::to_string(log)?;
        self.sink.write_json_line(&line)?;
        emitted.remember(key);
        Ok(true)
    }

    fn pending_backoff_for_round(&self, round: u32) -> std::time::Duration {
        let floor_ms = self
            .config
            .poll_interval_ms
            .max(PENDING_EXECUTION_BACKOFF_FLOOR_SECS.saturating_mul(1000));
        let factor = 1u64 << round.saturating_sub(1).min(4);
        std::time::Duration::from_millis(
            floor_ms
                .saturating_mul(factor)
                .min(PENDING_EXECUTION_BACKOFF_CAP_SECS.saturating_mul(1000)),
        )
    }

    fn should_wait_for_pending_retry(
        &self,
        block_hash: &str,
        now: std::time::Duration,
    ) -> Option<std::time::Duration> {
        self.pending_audits
            .lock()
            .unwrap()
            .next_retry_not_before(block_hash, now)
    }

    fn note_pending_recovery(&self, block: &BlockView) {
        let block_hash = format!("{:#x}", block.header.hash);
        let recovered = self.pending_audits.lock().unwrap().resolve(&block_hash);
        if let Some(recovered) = recovered {
            let elapsed_ms = self
                .clock
                .now()
                .saturating_sub(recovered.first_seen)
                .as_millis();
            eprintln!(
                "pending audit recovered height {} block {:#x}: rounds={} pending_for_ms={}",
                block.header.inner.number.value(),
                block.header.hash,
                recovered.rounds,
                elapsed_ms
            );
        }
    }

    fn log_pending_audit(
        &self,
        block: &BlockView,
        log: &AuditLog,
        block_attempt: u32,
        total_block_attempts: u32,
        non_retryable_execution: bool,
    ) {
        let mut diagnostic = log.clone();
        self.backfill_anomaly_details(&mut diagnostic);
        let failed: Vec<&str> = diagnostic
            .check_statuses()
            .iter()
            .filter_map(|(name, status)| (*status == CheckStatus::Fail).then_some(*name))
            .collect();
        let unresolved: Vec<&str> = diagnostic
            .check_statuses()
            .iter()
            .filter_map(|(name, status)| (*status == CheckStatus::Unknown).then_some(*name))
            .collect();
        let summary = diagnostic
            .details
            .as_ref()
            .map(|details| {
                details
                    .iter()
                    .map(|detail| {
                        let mut context = format!("{}:{}", detail.check_name, detail.error_code);
                        if let Some(tx_hash) = &detail.tx_hash {
                            context.push_str(&format!(" tx={tx_hash}"));
                        }
                        if let Some(input_index) = detail.input_index {
                            context.push_str(&format!(" input_index={input_index}"));
                        }
                        if let Some(out_point) = &detail.referenced_out_point {
                            context.push_str(&format!(" out_point={out_point}"));
                        }
                        if let Some(rpc_method) = &detail.rpc_method {
                            context.push_str(&format!(" rpc_method={rpc_method}"));
                        }
                        let reason = sanitize_diagnostic_reason(&detail.reason);
                        if !reason.is_empty() {
                            context.push_str(&format!(" reason={reason}"));
                        }
                        context
                    })
                    .take(6)
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        let fingerprint = format!("{failed:?}|{unresolved:?}|{summary}");
        let now = self.clock.now();
        let block_hash = format!("{:#x}", block.header.hash);
        let mut pending = self.pending_audits.lock().unwrap();
        let next_retry_not_before = non_retryable_execution
            .then(|| now + self.pending_backoff_for_round(pending.next_round(&block_hash)));
        let observation =
            pending.observe_pending(now, &block_hash, fingerprint, next_retry_not_before);
        drop(pending);
        if !observation.should_log {
            return;
        }
        let pending_for_ms = now.saturating_sub(observation.first_seen).as_millis();
        let next_retry_suffix = next_retry_not_before
            .map(|deadline| {
                format!(
                    " next_retry_in_ms={}",
                    deadline.saturating_sub(now).as_millis()
                )
            })
            .unwrap_or_default();
        let repeat_suffix = if observation.reason_changed {
            " reason_changed=true".to_string()
        } else if observation.repeats_since_change > 0 {
            format!(" repeat_count={}", observation.repeats_since_change)
        } else {
            String::new()
        };
        eprintln!(
            "pending audit height {} block {:#x}: audit_round={} block_attempt {}/{} pending_for_ms={} failed_checks={:?} unresolved_checks={:?} diagnostics=[{}]{}{}",
            block.header.inner.number.value(),
            block.header.hash,
            observation.rounds,
            block_attempt,
            total_block_attempts,
            pending_for_ms,
            failed,
            unresolved,
            summary,
            repeat_suffix,
            next_retry_suffix
        );
    }

    async fn ensure_cursor_genesis(
        &self,
        cursor: &mut CursorState,
        consensus: &ConsensusSnapshot,
    ) -> Result<bool> {
        let actual_genesis = format!("{:#x}", consensus.genesis_hash);
        match cursor.genesis_hash.as_deref() {
            Some(saved) if saved.eq_ignore_ascii_case(&actual_genesis) => Ok(false),
            Some(saved) => Err(anyhow!(
                "cursor genesis hash '{}' does not match selected rpc genesis '{}' (consensus id '{}')",
                saved,
                actual_genesis,
                consensus.consensus_id
            )),
            None => {
                if cursor.history.is_empty() {
                    return Err(anyhow!(
                        "legacy cursor file is missing genesis_hash and cannot be migrated safely without canonical history; use a fresh cursor file for this rpc"
                    ));
                }
                for (height, expected_hash) in &cursor.history {
                    let Some(header) = self.rpc.get_header_by_number(*height).await? else {
                        return Err(anyhow!(
                            "legacy cursor file is missing genesis_hash and cannot be migrated safely because height {} is unavailable from the selected rpc; use a fresh cursor file",
                            height
                        ));
                    };
                    let actual_hash = format!("{:#x}", header.hash);
                    if !actual_hash.eq_ignore_ascii_case(expected_hash) {
                        return Err(anyhow!(
                            "legacy cursor file is missing genesis_hash and cannot be migrated safely because recorded height {} hash '{}' does not match selected rpc hash '{}'; use a fresh cursor file",
                            height,
                            expected_hash,
                            actual_hash
                        ));
                    }
                }
                cursor.genesis_hash = Some(actual_genesis);
                Ok(true)
            }
        }
    }

    async fn poll_once(&self, cursor: &mut Option<CursorState>) -> Result<()> {
        let consensus = self
            .consensus()
            .await
            .context("consensus prerequisite failed")?;
        let tip = self
            .rpc
            .get_tip_header()
            .await?
            .ok_or_else(|| anyhow!("tip header missing"))?;

        let tip_height = tip.inner.number.value();
        if let Some(state) = cursor.as_mut()
            && self.ensure_cursor_genesis(state, &consensus).await?
        {
            self.save_cursor(state).await?;
        }

        let start_height = if let Some(state) = cursor.as_ref() {
            let completed_start = if state.last_hash.is_empty() && state.history.is_empty() {
                state.next_height.unwrap_or(tip_height)
            } else {
                self.resolve_common_ancestor(state).await? + 1
            };
            state
                .next_height
                .map_or(completed_start, |pending| pending.min(completed_start))
        } else {
            tip_height
        };
        if tip_height < start_height {
            self.maybe_report_stats(tip_height, cursor.as_ref().map(|state| state.last_height));
            return Ok(());
        }

        let mut latest_completed = cursor.as_ref().map(|state| state.last_height);
        for height in start_height..=tip_height {
            let pending_hash = cursor.as_ref().and_then(|state| {
                (state.next_height == Some(height))
                    .then(|| state.next_hash.clone())
                    .flatten()
            });
            if let Some(saved_hash) = pending_hash.as_deref()
                && let Some(_next_retry_at) =
                    self.should_wait_for_pending_retry(saved_hash, self.clock.now())
            {
                break;
            }
            let Some(outcome) = self.audit_height_with_retries(height, &consensus).await? else {
                break;
            };
            let (
                block,
                log,
                completed,
                block_attempt,
                total_block_attempts,
                non_retryable_execution,
            ) = match outcome {
                HeightAuditOutcome::Finalized { block, log } => {
                    (block, log, true, None, None, false)
                }
                HeightAuditOutcome::Pending {
                    block,
                    log,
                    block_attempt,
                    total_block_attempts,
                    non_retryable_execution,
                } => (
                    block,
                    log,
                    false,
                    Some(block_attempt),
                    Some(total_block_attempts),
                    non_retryable_execution,
                ),
            };
            let hash = format!("{:#x}", block.header.hash);
            if pending_hash
                .as_deref()
                .is_some_and(|saved| !saved.eq_ignore_ascii_case(&hash))
            {
                eprintln!(
                    "pending retry block changed on canonical chain at height {}: old={} new={}",
                    height,
                    pending_hash.unwrap_or_default(),
                    hash
                );
            }
            if completed {
                latest_completed = Some(height);
                self.note_pending_recovery(&block);
                self.write_final_audit_once(&log)?;
                if let Some(state) = cursor.as_mut() {
                    state.push_block(height, hash, self.config.history_retention);
                    self.save_cursor(state).await?;
                } else {
                    let state = CursorState::new(
                        format!("{:#x}", consensus.genesis_hash),
                        height,
                        hash,
                        self.config.history_retention,
                    );
                    self.save_cursor(&state).await?;
                    *cursor = Some(state);
                    eprintln!("initialized cursor at audited tip height {}", height);
                }
            } else {
                self.log_pending_audit(
                    &block,
                    &log,
                    block_attempt.unwrap_or(1),
                    total_block_attempts.unwrap_or(self.config.max_retries.saturating_add(1)),
                    non_retryable_execution,
                );
                if let Some(state) = cursor.as_mut() {
                    state.mark_pending(&block, self.config.history_retention);
                    self.save_cursor(state).await?;
                } else {
                    let state = CursorState::new_pending(
                        format!("{:#x}", consensus.genesis_hash),
                        &block,
                        self.config.history_retention,
                    );
                    self.save_cursor(&state).await?;
                    *cursor = Some(state);
                }
                break;
            }
        }

        self.maybe_report_stats(tip_height, latest_completed);

        Ok(())
    }

    async fn resolve_common_ancestor(&self, state: &CursorState) -> Result<u64> {
        if state.last_hash.is_empty() {
            return Ok(state
                .next_height
                .unwrap_or(state.last_height)
                .saturating_sub(1));
        }
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

    async fn consensus(&self) -> Result<ConsensusSnapshot> {
        if let Some(cached) = self.consensus_cache.lock().await.clone() {
            return Ok(cached);
        }

        let fetched = ConsensusSnapshot::from_rpc(&self.config, self.rpc.get_consensus().await?)
            .context("invalid node consensus")?;
        *self.consensus_cache.lock().await = Some(fetched.clone());
        Ok(fetched)
    }

    async fn audit_height_with_retries(
        &self,
        height: u64,
        consensus: &ConsensusSnapshot,
    ) -> Result<Option<HeightAuditOutcome>> {
        let total_attempts = self.config.max_retries.saturating_add(1);
        for attempt in 1..=total_attempts {
            let Some(block) = self.rpc.get_block_by_number(height).await? else {
                eprintln!("missing block at height {height}, stop this round");
                return Ok(None);
            };
            self.cache_block(&block);
            let mut log = self
                .audit_block_with_consensus_once(&block, consensus)
                .await;
            let unresolved = log.has_unresolved_checks();
            let non_retryable = log.has_non_retryable_execution_failure();
            if !unresolved {
                log.finalize(attempt, self.config.max_retries);
                return Ok(Some(HeightAuditOutcome::Finalized { block, log }));
            }
            if self.rpc.has_rate_limit_cooldown() {
                return Ok(Some(HeightAuditOutcome::Pending {
                    block,
                    log,
                    block_attempt: attempt,
                    total_block_attempts: total_attempts,
                    non_retryable_execution: false,
                }));
            }
            if !non_retryable && attempt < total_attempts {
                self.wait_for_retry(attempt, height, &block).await?;
                continue;
            }
            return Ok(Some(HeightAuditOutcome::Pending {
                block,
                log,
                block_attempt: attempt,
                total_block_attempts: total_attempts,
                non_retryable_execution: non_retryable,
            }));
        }
        unreachable!("retry loop must return before exhaustion")
    }

    async fn wait_for_retry(&self, attempt: u32, height: u64, block: &BlockView) -> Result<()> {
        let delay = retry_backoff(attempt);
        eprintln!(
            "retrying height {} block {:#x} after {} ms (attempt {}/{})",
            height,
            block.header.hash,
            delay.as_millis(),
            attempt + 1,
            self.config.max_retries.saturating_add(1)
        );
        self.clock
            .sleep(delay, &self.shutdown, "during retry backoff")
            .await
    }

    fn push_unknown_detail(
        &self,
        log: &mut AuditLog,
        check_name: &str,
        error_code: &str,
        reason: String,
    ) {
        log.push_detail(
            &self.config,
            DetailItem {
                check_name: check_name.to_string(),
                status: CheckStatus::Unknown,
                error_code: error_code.to_string(),
                failure_kind: None,
                rpc_method: None,
                attempts: None,
                max_retries: None,
                tx_hash: None,
                tx_index: None,
                input_index: None,
                output_index: None,
                referenced_out_point: None,
                expected_operator: None,
                expected_value: None,
                actual_value: None,
                unit: None,
                reason,
            },
        );
    }

    fn backfill_anomaly_details(&self, log: &mut AuditLog) {
        for (check_name, status) in [
            ("check_block_height", log.check_block_height),
            ("check_parent_hash", log.check_parent_hash),
            ("check_epoch_continuity", log.check_epoch_continuity),
            ("check_timestamp", log.check_timestamp),
            ("check_block_version", log.check_block_version),
            ("check_block_size", log.check_block_size),
            ("check_uncle_count_limit", log.check_uncle_count_limit),
            ("check_proposal_limit", log.check_proposal_limit),
            ("check_block_hash", log.check_block_hash),
            ("check_transaction_hashes", log.check_transaction_hashes),
            ("check_transactions_root", log.check_transactions_root),
            ("check_proposals_hash", log.check_proposals_hash),
            ("check_extra_hash", log.check_extra_hash),
            (
                "check_duplicate_transactions",
                log.check_duplicate_transactions,
            ),
            ("check_duplicate_proposals", log.check_duplicate_proposals),
            ("check_cellbase_structure", log.check_cellbase_structure),
            ("check_transaction_version", log.check_transaction_version),
            (
                "check_inputs_outputs_structure",
                log.check_inputs_outputs_structure,
            ),
            ("check_outputs_data_length", log.check_outputs_data_length),
            (
                "check_output_lock_hash_type",
                log.check_output_lock_hash_type,
            ),
            ("check_duplicate_cell_deps", log.check_duplicate_cell_deps),
            (
                "check_duplicate_header_deps",
                log.check_duplicate_header_deps,
            ),
            (
                "check_duplicate_inputs_in_transaction",
                log.check_duplicate_inputs_in_transaction,
            ),
            (
                "check_duplicate_inputs_in_block",
                log.check_duplicate_inputs_in_block,
            ),
            (
                "check_input_content_resolution",
                log.check_input_content_resolution,
            ),
            ("check_input_output_index", log.check_input_output_index),
            ("check_occupied_capacity", log.check_occupied_capacity),
            (
                "check_ordinary_capacity_conservation",
                log.check_ordinary_capacity_conservation,
            ),
            (
                "check_cellbase_reward_amount",
                log.check_cellbase_reward_amount,
            ),
            (
                "check_cellbase_reward_target",
                log.check_cellbase_reward_target,
            ),
            (
                "check_dao_withdraw_capacity",
                log.check_dao_withdraw_capacity,
            ),
        ] {
            if matches!(status, CheckStatus::Fail | CheckStatus::Unknown)
                && !log.has_detail_for(check_name)
            {
                let (error_code, reason) = match status {
                    CheckStatus::Fail => ("CHECK_FAILED", format!("{check_name} failed")),
                    CheckStatus::Unknown => (
                        "CHECK_INCOMPLETE",
                        format!("{check_name} could not be completed with the available rpc data"),
                    ),
                    _ => continue,
                };
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: check_name.to_string(),
                        status,
                        error_code: error_code.to_string(),
                        failure_kind: None,
                        rpc_method: None,
                        attempts: None,
                        max_retries: None,
                        tx_hash: None,
                        tx_index: None,
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: None,
                        expected_value: None,
                        actual_value: None,
                        unit: None,
                        reason,
                    },
                );
            }
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    async fn audit_block(&self, block: &BlockView) -> AuditLog {
        match self.consensus().await {
            Ok(consensus) => {
                let mut log = self
                    .audit_block_with_consensus_once(block, &consensus)
                    .await;
                log.finalize(1, self.config.max_retries);
                log
            }
            Err(err) => {
                let mut log = self.audit_block_without_consensus_once(block, &err).await;
                log.finalize(1, self.config.max_retries);
                log
            }
        }
    }

    async fn audit_block_without_consensus_once(
        &self,
        block: &BlockView,
        err: &anyhow::Error,
    ) -> AuditLog {
        let started = Instant::now();
        let mut log = AuditLog::new(&self.config, block);
        log.canonical_at_audit = matches!(
            self.rpc
                .get_header_by_number(block.header.inner.number.value())
                .await,
            Ok(Some(current)) if current.hash == block.header.hash
        );
        let core_block: CoreBlockView = block.clone().into();

        self.audit_header_and_block(block, &core_block, &mut log, None)
            .await;
        self.audit_transactions(block, &core_block, &mut log, None)
            .await;
        self.audit_reward(block, &mut log, None).await;

        let reason = format!("get_consensus unavailable: {err}");
        for check_name in [
            "check_timestamp",
            "check_block_version",
            "check_block_size",
            "check_uncle_count_limit",
            "check_proposal_limit",
            "check_transaction_version",
            "check_ordinary_capacity_conservation",
            "check_cellbase_reward_amount",
            "check_cellbase_reward_target",
            "check_dao_withdraw_capacity",
        ] {
            self.push_unknown_detail(
                &mut log,
                check_name,
                "CONSENSUS_UNAVAILABLE",
                reason.clone(),
            );
        }

        self.backfill_anomaly_details(&mut log);
        log.audit_duration_ms = started.elapsed().as_millis() as u64;
        log
    }

    async fn audit_block_with_consensus_once(
        &self,
        block: &BlockView,
        consensus: &ConsensusSnapshot,
    ) -> AuditLog {
        let started = Instant::now();
        let mut log = AuditLog::new(&self.config, block);
        log.canonical_at_audit = matches!(
            self.rpc
                .get_header_by_number(block.header.inner.number.value())
                .await,
            Ok(Some(current)) if current.hash == block.header.hash
        );
        let core_block: CoreBlockView = block.clone().into();
        self.audit_header_and_block(block, &core_block, &mut log, Some(consensus))
            .await;
        self.audit_transactions(block, &core_block, &mut log, Some(consensus))
            .await;
        self.audit_reward(block, &mut log, Some(consensus)).await;
        self.backfill_anomaly_details(&mut log);
        log.audit_duration_ms = started.elapsed().as_millis() as u64;
        log
    }

    async fn audit_header_and_block(
        &self,
        block: &BlockView,
        core_block: &CoreBlockView,
        log: &mut AuditLog,
        consensus: Option<&ConsensusSnapshot>,
    ) {
        self.cache_header(&block.header);
        let header_number = block.header.inner.number.value();

        if header_number == 0 {
            log.check_block_height = CheckStatus::Pass;
            log.check_parent_hash = CheckStatus::NotApplicable;
            log.check_epoch_continuity = CheckStatus::Pass;
            log.check_timestamp = CheckStatus::Pass;
        } else {
            match self
                .get_header_cached(&block.header.inner.parent_hash)
                .await
            {
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
                                failure_kind: None,
                                rpc_method: None,
                                attempts: None,
                                max_retries: None,
                                tx_hash: None,
                                tx_index: None,
                                input_index: None,
                                output_index: None,
                                referenced_out_point: None,
                                expected_operator: Some("equal".to_string()),
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
                        log.push_detail(
                            &self.config,
                            DetailItem {
                                check_name: "check_parent_hash".to_string(),
                                status: CheckStatus::Fail,
                                error_code: "PARENT_HASH_MISMATCH".to_string(),
                                failure_kind: None,
                                rpc_method: None,
                                attempts: None,
                                max_retries: None,
                                tx_hash: None,
                                tx_index: None,
                                input_index: None,
                                output_index: None,
                                referenced_out_point: None,
                                expected_operator: Some("equal".to_string()),
                                expected_value: Some(format!(
                                    "{:#x}",
                                    block.header.inner.parent_hash
                                )),
                                actual_value: Some(format!("{parent_recomputed:#x}")),
                                unit: Some("hash".to_string()),
                                reason:
                                    "block parent_hash must match the recomputed parent header hash"
                                        .to_string(),
                            },
                        );
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
                                failure_kind: None,
                                rpc_method: None,
                                attempts: None,
                                max_retries: None,
                                tx_hash: None,
                                tx_index: None,
                                input_index: None,
                                output_index: None,
                                referenced_out_point: None,
                                expected_operator: None,
                                expected_value: None,
                                actual_value: None,
                                unit: None,
                                reason,
                            },
                        );
                    }

                    if let Some(consensus) = consensus {
                        let mut timestamps = vec![parent.inner.timestamp.value()];
                        let mut walk_hash = parent.inner.parent_hash;
                        let mut complete = true;
                        for _ in 0..consensus.median_time_block_count.saturating_sub(1) {
                            if walk_hash == H256::default() {
                                break;
                            }
                            match self.get_header_cached(&walk_hash).await {
                                Ok(Some(h)) => {
                                    timestamps.push(h.inner.timestamp.value());
                                    walk_hash = h.inner.parent_hash;
                                }
                                Ok(None) => {
                                    complete = false;
                                    break;
                                }
                                Err(err) => {
                                    complete = false;
                                    self.push_unknown_detail(
                                        log,
                                        "check_timestamp",
                                        "TIMESTAMP_ANCESTOR_UNAVAILABLE",
                                        format!("timestamp ancestor lookup failed: {err}"),
                                    );
                                    break;
                                }
                            }
                        }
                        if complete || timestamps.len() == consensus.median_time_block_count {
                            timestamps.sort_unstable();
                            let median = timestamps[timestamps.len() / 2];
                            let ts = block.header.inner.timestamp.value();
                            let now_ms = Utc::now().timestamp_millis().max(0) as u64;
                            if ts > median && ts <= now_ms.saturating_add(self.config.max_future_ms)
                            {
                                log.check_timestamp = CheckStatus::Pass;
                            } else {
                                log.check_timestamp = CheckStatus::Fail;
                                log.push_detail(
                                    &self.config,
                                    DetailItem {
                                        check_name: "check_timestamp".to_string(),
                                        status: CheckStatus::Fail,
                                        error_code: "TIMESTAMP_OUT_OF_RANGE".to_string(),
                       failure_kind: None,
                       rpc_method: None,
                       attempts: None,
                       max_retries: None,
                                        tx_hash: None,
                                        tx_index: None,
                                        input_index: None,
                                        output_index: None,
                                        referenced_out_point: None,
                                        expected_operator: Some("greater_than_median_and_not_too_future".to_string()),
                                        expected_value: Some(format!(
                                            "median={} max_future={}",
                                            median,
                                            now_ms.saturating_add(self.config.max_future_ms)
                                        )),
                                        actual_value: Some(ts.to_string()),
                                        unit: Some("ms".to_string()),
                                        reason: "block timestamp must be greater than the ancestor median and not exceed the configured future allowance".to_string(),
                                    },
                                );
                            }
                        } else {
                            log.check_timestamp = CheckStatus::Unknown;
                            self.push_unknown_detail(
                                log,
                                "check_timestamp",
                                "TIMESTAMP_MEDIAN_INCOMPLETE",
                                format!(
                                    "need {} ancestor timestamps, resolved {}",
                                    consensus.median_time_block_count,
                                    timestamps.len()
                                ),
                            );
                        }
                    } else {
                        log.check_timestamp = CheckStatus::Unknown;
                        self.push_unknown_detail(
                            log,
                            "check_timestamp",
                            "CONSENSUS_UNAVAILABLE",
                            "timestamp validation requires get_consensus parameters".to_string(),
                        );
                    }
                }
                Ok(None) => {
                    log.check_block_height = CheckStatus::Unknown;
                    log.check_parent_hash = CheckStatus::Unknown;
                    log.check_epoch_continuity = CheckStatus::Unknown;
                    log.check_timestamp = CheckStatus::Unknown;
                    for check_name in [
                        "check_block_height",
                        "check_parent_hash",
                        "check_epoch_continuity",
                        "check_timestamp",
                    ] {
                        self.push_unknown_detail(
                            log,
                            check_name,
                            "PARENT_HEADER_MISSING",
                            format!(
                                "parent header {:#x} is unavailable",
                                block.header.inner.parent_hash
                            ),
                        );
                    }
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
                                failure_kind: None,
                                rpc_method: None,
                                attempts: None,
                                max_retries: None,
                                tx_hash: None,
                                tx_index: None,
                                input_index: None,
                                output_index: None,
                                referenced_out_point: None,
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
        if let Some(consensus) = consensus {
            let actual_block_version = block.header.inner.version.value();
            log.check_block_version = if actual_block_version == consensus.block_version {
                CheckStatus::Pass
            } else {
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_block_version".to_string(),
                        status: CheckStatus::Fail,
                        error_code: "BLOCK_VERSION_MISMATCH".to_string(),
                        failure_kind: None,
                        rpc_method: None,
                        attempts: None,
                        max_retries: None,
                        tx_hash: None,
                        tx_index: None,
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: Some("equal".to_string()),
                        expected_value: Some(consensus.block_version.to_string()),
                        actual_value: Some(actual_block_version.to_string()),
                        unit: Some("version".to_string()),
                        reason: "block version does not match the consensus block_version"
                            .to_string(),
                    },
                );
                CheckStatus::Fail
            };
            let actual_uncle_count = block.uncles.len();
            log.check_uncle_count_limit = if actual_uncle_count <= consensus.max_uncles_num {
                CheckStatus::Pass
            } else {
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_uncle_count_limit".to_string(),
                        status: CheckStatus::Fail,
                        error_code: "UNCLE_COUNT_EXCEEDED".to_string(),
                        failure_kind: None,
                        rpc_method: None,
                        attempts: None,
                        max_retries: None,
                        tx_hash: None,
                        tx_index: None,
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: Some("less_than_or_equal".to_string()),
                        expected_value: Some(consensus.max_uncles_num.to_string()),
                        actual_value: Some(actual_uncle_count.to_string()),
                        unit: Some("uncle".to_string()),
                        reason: "uncle count exceeds the consensus max_uncles_num limit"
                            .to_string(),
                    },
                );
                CheckStatus::Fail
            };
            log.check_block_size = if actual_block_size <= consensus.max_block_bytes {
                CheckStatus::Pass
            } else {
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_block_size".to_string(),
                        status: CheckStatus::Fail,
                        error_code: "BLOCK_SIZE_EXCEEDED".to_string(),
                        failure_kind: None,
                        rpc_method: None,
                        attempts: None,
                        max_retries: None,
                        tx_hash: None,
                        tx_index: None,
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: Some("less_than_or_equal".to_string()),
                        expected_value: Some(consensus.max_block_bytes.to_string()),
                        actual_value: Some(actual_block_size.to_string()),
                        unit: Some("byte".to_string()),
                        reason: "block serialized size exceeds the consensus max_block_bytes limit"
                            .to_string(),
                    },
                );
                CheckStatus::Fail
            };
            log.check_proposal_limit = if block.proposals.len() <= consensus.proposal_limit {
                CheckStatus::Pass
            } else {
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_proposal_limit".to_string(),
                        status: CheckStatus::Fail,
                        error_code: "PROPOSAL_LIMIT_EXCEEDED".to_string(),
                        failure_kind: None,
                        rpc_method: None,
                        attempts: None,
                        max_retries: None,
                        tx_hash: None,
                        tx_index: None,
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: Some("less_than_or_equal".to_string()),
                        expected_value: Some(consensus.proposal_limit.to_string()),
                        actual_value: Some(block.proposals.len().to_string()),
                        unit: Some("proposal".to_string()),
                        reason: "proposal count exceeds the consensus max_block_proposals_limit"
                            .to_string(),
                    },
                );
                CheckStatus::Fail
            };
        } else {
            log.check_block_version = CheckStatus::Unknown;
            log.check_block_size = CheckStatus::Unknown;
            log.check_uncle_count_limit = CheckStatus::Unknown;
            log.check_proposal_limit = CheckStatus::Unknown;
        };

        let computed_header_hash: H256 = core_block.data().header().calc_header_hash().unpack();
        log.check_block_hash = if computed_header_hash == block.header.hash {
            CheckStatus::Pass
        } else {
            log.push_detail(
                &self.config,
                DetailItem {
                    check_name: "check_block_hash".to_string(),
                    status: CheckStatus::Fail,
                    error_code: "BLOCK_HASH_MISMATCH".to_string(),
                    failure_kind: None,
                    rpc_method: None,
                    attempts: None,
                    max_retries: None,
                    tx_hash: None,
                    tx_index: None,
                    input_index: None,
                    output_index: None,
                    referenced_out_point: None,
                    expected_operator: Some("equal".to_string()),
                    expected_value: Some(format!("{:#x}", block.header.hash)),
                    actual_value: Some(format!("{computed_header_hash:#x}")),
                    unit: Some("hash".to_string()),
                    reason:
                        "recomputed block header hash does not match the rpc-provided block hash"
                            .to_string(),
                },
            );
            CheckStatus::Fail
        };

        let computed_tx_hashes = block_data.calc_tx_hashes();
        let mut tx_hash_status = true;
        for (tx_index, (computed, tx)) in computed_tx_hashes
            .iter()
            .zip(block.transactions.iter())
            .enumerate()
        {
            let computed: H256 = computed.unpack();
            if computed != tx.hash {
                tx_hash_status = false;
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_transaction_hashes".to_string(),
                        status: CheckStatus::Fail,
                        error_code: "TRANSACTION_HASH_MISMATCH".to_string(),
                       failure_kind: None,
                       rpc_method: None,
                       attempts: None,
                       max_retries: None,
                        tx_hash: Some(format!("{:#x}", tx.hash)),
                        tx_index: Some(tx_index),
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: Some("equal".to_string()),
                        expected_value: Some(format!("{:#x}", tx.hash)),
                        actual_value: Some(format!("{computed:#x}")),
                        unit: Some("hash".to_string()),
                        reason: "recomputed transaction hash does not match the rpc-provided transaction hash"
                            .to_string(),
                    },
                );
            }
        }
        log.check_transaction_hashes = if tx_hash_status {
            CheckStatus::Pass
        } else {
            CheckStatus::Fail
        };

        let computed_transactions_root: H256 = core_block.calc_transactions_root().unpack();
        log.check_transactions_root = if computed_transactions_root
            == block.header.inner.transactions_root
        {
            CheckStatus::Pass
        } else {
            log.push_detail(
                &self.config,
                DetailItem {
                    check_name: "check_transactions_root".to_string(),
                    status: CheckStatus::Fail,
                    error_code: "TRANSACTIONS_ROOT_MISMATCH".to_string(),
                    failure_kind: None,
                    rpc_method: None,
                    attempts: None,
                    max_retries: None,
                    tx_hash: None,
                    tx_index: None,
                    input_index: None,
                    output_index: None,
                    referenced_out_point: None,
                    expected_operator: Some("equal".to_string()),
                    expected_value: Some(format!("{:#x}", block.header.inner.transactions_root)),
                    actual_value: Some(format!("{computed_transactions_root:#x}")),
                    unit: Some("hash".to_string()),
                    reason: "recomputed transactions_root does not match the header field"
                        .to_string(),
                },
            );
            CheckStatus::Fail
        };

        let computed_proposals_hash: H256 = block_data.calc_proposals_hash().unpack();
        log.check_proposals_hash = if computed_proposals_hash == block.header.inner.proposals_hash {
            CheckStatus::Pass
        } else {
            log.push_detail(
                &self.config,
                DetailItem {
                    check_name: "check_proposals_hash".to_string(),
                    status: CheckStatus::Fail,
                    error_code: "PROPOSALS_HASH_MISMATCH".to_string(),
                    failure_kind: None,
                    rpc_method: None,
                    attempts: None,
                    max_retries: None,
                    tx_hash: None,
                    tx_index: None,
                    input_index: None,
                    output_index: None,
                    referenced_out_point: None,
                    expected_operator: Some("equal".to_string()),
                    expected_value: Some(format!("{:#x}", block.header.inner.proposals_hash)),
                    actual_value: Some(format!("{computed_proposals_hash:#x}")),
                    unit: Some("hash".to_string()),
                    reason: "recomputed proposals_hash does not match the header field".to_string(),
                },
            );
            CheckStatus::Fail
        };

        let computed_extra_hash: H256 = core_block.calc_extra_hash().extra_hash().unpack();
        log.check_extra_hash = if computed_extra_hash == block.header.inner.extra_hash {
            CheckStatus::Pass
        } else {
            log.push_detail(
                &self.config,
                DetailItem {
                    check_name: "check_extra_hash".to_string(),
                    status: CheckStatus::Fail,
                    error_code: "EXTRA_HASH_MISMATCH".to_string(),
                    failure_kind: None,
                    rpc_method: None,
                    attempts: None,
                    max_retries: None,
                    tx_hash: None,
                    tx_index: None,
                    input_index: None,
                    output_index: None,
                    referenced_out_point: None,
                    expected_operator: Some("equal".to_string()),
                    expected_value: Some(format!("{:#x}", block.header.inner.extra_hash)),
                    actual_value: Some(format!("{computed_extra_hash:#x}")),
                    unit: Some("hash".to_string()),
                    reason: "recomputed extra_hash does not match the header field".to_string(),
                },
            );
            CheckStatus::Fail
        };

        let mut tx_hash_set = HashSet::new();
        let mut tx_dup = false;
        for (tx_index, tx) in block.transactions.iter().enumerate() {
            let key = format!("{:#x}", tx.hash);
            if !tx_hash_set.insert(key) {
                tx_dup = true;
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_duplicate_transactions".to_string(),
                        status: CheckStatus::Fail,
                        error_code: "DUPLICATE_TRANSACTION".to_string(),
                        failure_kind: None,
                        rpc_method: None,
                        attempts: None,
                        max_retries: None,
                        tx_hash: Some(format!("{:#x}", tx.hash)),
                        tx_index: Some(tx_index),
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: None,
                        expected_value: None,
                        actual_value: Some(format!("{:#x}", tx.hash)),
                        unit: Some("hash".to_string()),
                        reason: "duplicate transaction hash detected within block".to_string(),
                    },
                );
            }
        }
        log.check_duplicate_transactions = if tx_dup {
            CheckStatus::Fail
        } else {
            CheckStatus::Pass
        };

        let mut proposal_set = HashSet::new();
        let mut proposal_dup = false;
        for proposal_index in 0..block.proposals.len() {
            let p = &block.proposals[proposal_index];
            let key = p.0.to_vec();
            if !proposal_set.insert(key) {
                proposal_dup = true;
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_duplicate_proposals".to_string(),
                        status: CheckStatus::Fail,
                        error_code: "DUPLICATE_PROPOSAL".to_string(),
                        failure_kind: None,
                        rpc_method: None,
                        attempts: None,
                        max_retries: None,
                        tx_hash: None,
                        tx_index: None,
                        input_index: None,
                        output_index: Some(proposal_index),
                        referenced_out_point: None,
                        expected_operator: None,
                        expected_value: None,
                        actual_value: Some(hex::encode(p.0)),
                        unit: Some("proposal".to_string()),
                        reason: "duplicate proposal short id detected within block".to_string(),
                    },
                );
            }
        }
        log.check_duplicate_proposals = if proposal_dup {
            CheckStatus::Fail
        } else {
            CheckStatus::Pass
        };

        self.check_cellbase_structure(block, log);
    }

    fn check_cellbase_structure(&self, block: &BlockView, log: &mut AuditLog) {
        let mut status = CheckStatus::Pass;
        let Some(first_tx) = block.transactions.first() else {
            log.push_detail(
                &self.config,
                DetailItem {
                    check_name: "check_cellbase_structure".to_string(),
                    status: CheckStatus::Fail,
                    error_code: "CELLBASE_MISSING".to_string(),
                    failure_kind: None,
                    rpc_method: None,
                    attempts: None,
                    max_retries: None,
                    tx_hash: None,
                    tx_index: None,
                    input_index: None,
                    output_index: None,
                    referenced_out_point: None,
                    expected_operator: None,
                    expected_value: None,
                    actual_value: None,
                    unit: None,
                    reason: "block must contain a cellbase transaction at index 0".to_string(),
                },
            );
            log.check_cellbase_structure = CheckStatus::Fail;
            return;
        };

        let first_packed: packed::Transaction = first_tx.inner.clone().into();
        if !first_packed.is_cellbase() {
            status = CheckStatus::Fail;
            log.push_detail(
                &self.config,
                DetailItem {
                    check_name: "check_cellbase_structure".to_string(),
                    status,
                    error_code: "CELLBASE_FIRST_TRANSACTION_INVALID".to_string(),
                    failure_kind: None,
                    rpc_method: None,
                    attempts: None,
                    max_retries: None,
                    tx_hash: Some(format!("{:#x}", first_tx.hash)),
                    tx_index: Some(0),
                    input_index: None,
                    output_index: None,
                    referenced_out_point: None,
                    expected_operator: None,
                    expected_value: None,
                    actual_value: None,
                    unit: None,
                    reason: "first transaction must be a valid cellbase".to_string(),
                },
            );
        }

        for (tx_index, tx) in block.transactions.iter().enumerate().skip(1) {
            let packed_tx: packed::Transaction = tx.inner.clone().into();
            if packed_tx.is_cellbase() {
                status = CheckStatus::Fail;
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_cellbase_structure".to_string(),
                        status,
                        error_code: "MULTIPLE_CELLBASE_TRANSACTIONS".to_string(),
                        failure_kind: None,
                        rpc_method: None,
                        attempts: None,
                        max_retries: None,
                        tx_hash: Some(format!("{:#x}", tx.hash)),
                        tx_index: Some(tx_index),
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: None,
                        expected_value: None,
                        actual_value: None,
                        unit: None,
                        reason: "only the first transaction may be cellbase".to_string(),
                    },
                );
            }
        }

        if first_tx.inner.outputs.len() > 1
            || first_tx.inner.outputs_data.len() > 1
            || first_tx.inner.outputs.len() != first_tx.inner.outputs_data.len()
        {
            status = CheckStatus::Fail;
            log.push_detail(
                &self.config,
                DetailItem {
                    check_name: "check_cellbase_structure".to_string(),
                    status,
                    error_code: "CELLBASE_OUTPUT_STRUCTURE_INVALID".to_string(),
                    failure_kind: None,
                    rpc_method: None,
                    attempts: None,
                    max_retries: None,
                    tx_hash: Some(format!("{:#x}", first_tx.hash)),
                    tx_index: Some(0),
                    input_index: None,
                    output_index: None,
                    referenced_out_point: None,
                    expected_operator: Some("single_output_with_matching_data".to_string()),
                    expected_value: Some("outputs<=1 and outputs==outputs_data".to_string()),
                    actual_value: Some(format!(
                        "outputs={} outputs_data={}",
                        first_tx.inner.outputs.len(),
                        first_tx.inner.outputs_data.len()
                    )),
                    unit: None,
                    reason:
                        "cellbase must have at most one output and matching outputs_data length"
                            .to_string(),
                },
            );
        }

        if let Some(input) = first_tx.inner.inputs.first() {
            let out_point: packed::OutPoint = input.previous_output.clone().into();
            if !out_point.is_null() {
                status = CheckStatus::Fail;
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_cellbase_structure".to_string(),
                        status,
                        error_code: "CELLBASE_PREVIOUS_OUTPUT_NOT_NULL".to_string(),
                        failure_kind: None,
                        rpc_method: None,
                        attempts: None,
                        max_retries: None,
                        tx_hash: Some(format!("{:#x}", first_tx.hash)),
                        tx_index: Some(0),
                        input_index: Some(0),
                        output_index: None,
                        referenced_out_point: Some(format!(
                            "{:#x}:{}",
                            input.previous_output.tx_hash,
                            input.previous_output.index.value()
                        )),
                        expected_operator: Some("equal".to_string()),
                        expected_value: Some("null_out_point".to_string()),
                        actual_value: None,
                        unit: None,
                        reason: "cellbase input previous_output must be null".to_string(),
                    },
                );
            }
            if input.since.value() != block.header.inner.number.value() {
                status = CheckStatus::Fail;
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_cellbase_structure".to_string(),
                        status,
                        error_code: "CELLBASE_SINCE_MISMATCH".to_string(),
                        failure_kind: None,
                        rpc_method: None,
                        attempts: None,
                        max_retries: None,
                        tx_hash: Some(format!("{:#x}", first_tx.hash)),
                        tx_index: Some(0),
                        input_index: Some(0),
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: Some("equal".to_string()),
                        expected_value: Some(block.header.inner.number.value().to_string()),
                        actual_value: Some(input.since.value().to_string()),
                        unit: Some("block".to_string()),
                        reason: "cellbase input since must equal the block number".to_string(),
                    },
                );
            }
        } else {
            status = CheckStatus::Fail;
            log.push_detail(
                &self.config,
                DetailItem {
                    check_name: "check_cellbase_structure".to_string(),
                    status,
                    error_code: "CELLBASE_INPUT_MISSING".to_string(),
                    failure_kind: None,
                    rpc_method: None,
                    attempts: None,
                    max_retries: None,
                    tx_hash: Some(format!("{:#x}", first_tx.hash)),
                    tx_index: Some(0),
                    input_index: None,
                    output_index: None,
                    referenced_out_point: None,
                    expected_operator: Some("equal".to_string()),
                    expected_value: Some("1".to_string()),
                    actual_value: Some("0".to_string()),
                    unit: Some("input".to_string()),
                    reason: "cellbase must contain exactly one input".to_string(),
                },
            );
        }

        if first_tx.inner.witnesses.len() != 1 {
            status = CheckStatus::Fail;
            log.push_detail(
                &self.config,
                DetailItem {
                    check_name: "check_cellbase_structure".to_string(),
                    status,
                    error_code: "CELLBASE_WITNESS_COUNT_INVALID".to_string(),
                    failure_kind: None,
                    rpc_method: None,
                    attempts: None,
                    max_retries: None,
                    tx_hash: Some(format!("{:#x}", first_tx.hash)),
                    tx_index: Some(0),
                    input_index: None,
                    output_index: None,
                    referenced_out_point: None,
                    expected_operator: Some("equal".to_string()),
                    expected_value: Some("1".to_string()),
                    actual_value: Some(first_tx.inner.witnesses.len().to_string()),
                    unit: Some("witness".to_string()),
                    reason: "cellbase must contain exactly one witness".to_string(),
                },
            );
        }

        if let Some(witness) = first_tx.inner.witnesses.first()
            && packed::CellbaseWitness::from_slice(witness.as_bytes()).is_err()
        {
            status = CheckStatus::Fail;
            log.push_detail(
                &self.config,
                DetailItem {
                    check_name: "check_cellbase_structure".to_string(),
                    status,
                    error_code: "CELLBASE_WITNESS_INVALID".to_string(),
                    failure_kind: None,
                    rpc_method: None,
                    attempts: None,
                    max_retries: None,
                    tx_hash: Some(format!("{:#x}", first_tx.hash)),
                    tx_index: Some(0),
                    input_index: None,
                    output_index: None,
                    referenced_out_point: None,
                    expected_operator: None,
                    expected_value: None,
                    actual_value: None,
                    unit: None,
                    reason: "cellbase witness cannot be decoded".to_string(),
                },
            );
        }

        if let Some(output) = first_tx.inner.outputs.first()
            && !output.type_.is_none()
        {
            status = CheckStatus::Fail;
            log.push_detail(
                &self.config,
                DetailItem {
                    check_name: "check_cellbase_structure".to_string(),
                    status,
                    error_code: "CELLBASE_TYPE_SCRIPT_PRESENT".to_string(),
                    failure_kind: None,
                    rpc_method: None,
                    attempts: None,
                    max_retries: None,
                    tx_hash: Some(format!("{:#x}", first_tx.hash)),
                    tx_index: Some(0),
                    input_index: None,
                    output_index: Some(0),
                    referenced_out_point: None,
                    expected_operator: Some("equal".to_string()),
                    expected_value: Some("none".to_string()),
                    actual_value: Some("present".to_string()),
                    unit: None,
                    reason: "cellbase output type script must be absent".to_string(),
                },
            );
        }
        if first_tx
            .inner
            .outputs_data
            .first()
            .is_some_and(|data| !data.as_bytes().is_empty())
        {
            status = CheckStatus::Fail;
            log.push_detail(
                &self.config,
                DetailItem {
                    check_name: "check_cellbase_structure".to_string(),
                    status,
                    error_code: "CELLBASE_OUTPUT_DATA_NOT_EMPTY".to_string(),
                    failure_kind: None,
                    rpc_method: None,
                    attempts: None,
                    max_retries: None,
                    tx_hash: Some(format!("{:#x}", first_tx.hash)),
                    tx_index: Some(0),
                    input_index: None,
                    output_index: Some(0),
                    referenced_out_point: None,
                    expected_operator: Some("equal".to_string()),
                    expected_value: Some("0".to_string()),
                    actual_value: Some(first_tx.inner.outputs_data[0].as_bytes().len().to_string()),
                    unit: Some("byte".to_string()),
                    reason: "cellbase output data must be empty".to_string(),
                },
            );
        }
        log.check_cellbase_structure = status;
    }

    async fn fetch_committed_transaction(&self, hash: &H256) -> CachedTransaction {
        match self.rpc.get_transaction(hash).await {
            Ok(Some(resp)) => {
                let tx_status = resp.tx_status.clone();
                if tx_status.status != Status::Committed {
                    return CachedTransaction::Unavailable(ResolutionIssue {
                        status: CheckStatus::Unknown,
                        error_code: "INPUT_TX_NOT_COMMITTED".to_string(),
                        reason: format!(
                            "get_transaction returned non-committed status {:?}",
                            tx_status.status
                        ),
                    });
                }

                let Some(block_hash) = tx_status.block_hash else {
                    return CachedTransaction::Unavailable(ResolutionIssue {
                        status: CheckStatus::Unknown,
                        error_code: "INPUT_TX_BLOCK_HASH_MISSING".to_string(),
                        reason: "committed transaction response missing tx_status.block_hash"
                            .to_string(),
                    });
                };

                let Some(tx) = resp
                    .transaction
                    .and_then(|r: ResponseFormat<TransactionView>| match r.inner {
                        Either::Left(v) => Some(v),
                        Either::Right(_) => None,
                    })
                else {
                    return CachedTransaction::Unavailable(ResolutionIssue {
                        status: CheckStatus::Unknown,
                        error_code: "INPUT_TX_JSON_MISSING".to_string(),
                        reason: "committed transaction response missing full JSON transaction"
                            .to_string(),
                    });
                };

                let recomputed: H256 = packed::Transaction::from(tx.inner.clone())
                    .calc_tx_hash()
                    .unpack();
                if recomputed != *hash {
                    return CachedTransaction::Unavailable(ResolutionIssue {
                        status: CheckStatus::Fail,
                        error_code: "INPUT_TX_HASH_MISMATCH".to_string(),
                        reason: format!(
                            "recomputed source transaction hash {:#x} does not match requested hash {hash:#x}",
                            recomputed
                        ),
                    });
                }

                CachedTransaction::Resolved(ResolvedCommittedTransaction {
                    tx,
                    block_hash,
                    block_number: tx_status.block_number.map(|number| number.value()),
                })
            }
            Ok(None) => CachedTransaction::Unavailable(ResolutionIssue {
                status: CheckStatus::Unknown,
                error_code: "INPUT_TX_MISSING".to_string(),
                reason: "get_transaction returned null".to_string(),
            }),
            Err(err) => CachedTransaction::Unavailable(ResolutionIssue {
                status: CheckStatus::Unknown,
                error_code: "INPUT_TX_RPC_ERROR".to_string(),
                reason: format!("get_transaction failed: {err}"),
            }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn push_input_detail(
        &self,
        log: &mut AuditLog,
        check_name: &str,
        status: CheckStatus,
        error_code: &str,
        tx_hash: &H256,
        tx_index: usize,
        input_index: usize,
        out_point: String,
        reason: String,
    ) {
        log.push_detail(
            &self.config,
            DetailItem {
                check_name: check_name.to_string(),
                status,
                error_code: error_code.to_string(),
                failure_kind: None,
                rpc_method: None,
                attempts: None,
                max_retries: None,
                tx_hash: Some(format!("{tx_hash:#x}")),
                tx_index: Some(tx_index),
                input_index: Some(input_index),
                output_index: None,
                referenced_out_point: Some(out_point),
                expected_operator: None,
                expected_value: None,
                actual_value: None,
                unit: None,
                reason,
            },
        );
    }

    fn output_uses_dao_type(
        output: &ckb_jsonrpc_types::CellOutput,
        consensus: &ConsensusSnapshot,
    ) -> bool {
        output.type_.as_ref().is_some_and(|script| {
            script.hash_type == ScriptHashType::Type && script.code_hash == consensus.dao_type_hash
        })
    }

    fn decode_dao_block_number(
        data: &ckb_jsonrpc_types::JsonBytes,
        error_code: &str,
        context: &str,
    ) -> std::result::Result<u64, ResolutionIssue> {
        let raw = data.as_bytes();
        if raw.len() != 8 {
            return Err(ResolutionIssue {
                status: CheckStatus::Fail,
                error_code: error_code.to_string(),
                reason: format!("{context} must be exactly 8 bytes, got {}", raw.len()),
            });
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(raw.as_ref());
        Ok(u64::from_le_bytes(bytes))
    }

    fn extract_dao_deposit_header_hash(
        current_tx: &TransactionView,
        input_index: usize,
    ) -> std::result::Result<H256, ResolutionIssue> {
        let Some(witness) = current_tx.inner.witnesses.get(input_index) else {
            return Err(ResolutionIssue {
                status: CheckStatus::Fail,
                error_code: "DAO_DEPOSIT_HEADER_INDEX_MISSING".to_string(),
                reason: format!(
                    "dao withdrawing input {} is missing its witness",
                    input_index
                ),
            });
        };
        let witness_args =
            packed::WitnessArgs::from_slice(witness.as_bytes()).map_err(|_| ResolutionIssue {
                status: CheckStatus::Fail,
                error_code: "DAO_WITNESS_INVALID".to_string(),
                reason: format!(
                    "dao withdrawing input {} witness cannot be decoded as WitnessArgs",
                    input_index
                ),
            })?;
        let input_type = witness_args
            .input_type()
            .to_opt()
            .ok_or_else(|| ResolutionIssue {
                status: CheckStatus::Fail,
                error_code: "DAO_DEPOSIT_HEADER_INDEX_MISSING".to_string(),
                reason: format!(
                    "dao withdrawing input {} witness is missing input_type deposit header index",
                    input_index
                ),
            })?;
        let raw = input_type.raw_data();
        if raw.len() != 8 {
            return Err(ResolutionIssue {
                status: CheckStatus::Fail,
                error_code: "DAO_DEPOSIT_HEADER_INDEX_INVALID".to_string(),
                reason: format!(
                    "dao withdrawing input {} witness input_type must be exactly 8 bytes, got {}",
                    input_index,
                    raw.len()
                ),
            });
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(raw.as_ref());
        let header_dep_index =
            usize::try_from(u64::from_le_bytes(bytes)).map_err(|_| ResolutionIssue {
                status: CheckStatus::Fail,
                error_code: "DAO_DEPOSIT_HEADER_INDEX_INVALID".to_string(),
                reason: format!(
                    "dao withdrawing input {} witness input_type index exceeds platform usize",
                    input_index
                ),
            })?;
        current_tx
            .inner
            .header_deps
            .get(header_dep_index)
            .cloned()
            .ok_or_else(|| ResolutionIssue {
                status: CheckStatus::Fail,
                error_code: "DAO_DEPOSIT_HEADER_REFERENCE_MISSING".to_string(),
                reason: format!(
                    "dao withdrawing input {} references header_dep index {} but only {} header_deps exist",
                    input_index,
                    header_dep_index,
                    current_tx.inner.header_deps.len()
                ),
            })
    }

    fn validate_dao_withdrawing_output(
        &self,
        consensus: &ConsensusSnapshot,
        current_tx: &TransactionView,
        input_index: usize,
        input_capacity: u64,
        deposited_block_number: u64,
    ) -> std::result::Result<(), ResolutionIssue> {
        let Some(output) = current_tx.inner.outputs.get(input_index) else {
            return Err(ResolutionIssue {
                status: CheckStatus::Fail,
                error_code: "DAO_WITHDRAWING_OUTPUT_MISSING".to_string(),
                reason: format!(
                    "dao deposit input {} requires a same-index withdrawing output",
                    input_index
                ),
            });
        };
        if !Self::output_uses_dao_type(output, consensus) {
            return Err(ResolutionIssue {
                status: CheckStatus::Fail,
                error_code: "DAO_WITHDRAWING_OUTPUT_TYPE_INVALID".to_string(),
                reason: format!(
                    "dao deposit input {} requires a same-index DAO type output",
                    input_index
                ),
            });
        }
        if output.capacity.value() != input_capacity {
            return Err(ResolutionIssue {
                status: CheckStatus::Fail,
                error_code: "DAO_WITHDRAWING_OUTPUT_CAPACITY_MISMATCH".to_string(),
                reason: format!(
                    "dao withdrawing output {} capacity {} does not match deposited input capacity {}",
                    input_index,
                    output.capacity.value(),
                    input_capacity
                ),
            });
        }
        let Some(output_data) = current_tx.inner.outputs_data.get(input_index) else {
            return Err(ResolutionIssue {
                status: CheckStatus::Fail,
                error_code: "DAO_WITHDRAWING_OUTPUT_DATA_MISSING".to_string(),
                reason: format!(
                    "dao withdrawing output {} is missing 8-byte deposited block number data",
                    input_index
                ),
            });
        };
        let stored_block_number = Self::decode_dao_block_number(
            output_data,
            "DAO_WITHDRAWING_OUTPUT_DATA_INVALID",
            "dao withdrawing output data",
        )?;
        if stored_block_number != deposited_block_number {
            return Err(ResolutionIssue {
                status: CheckStatus::Fail,
                error_code: "DAO_WITHDRAWING_OUTPUT_BLOCK_NUMBER_MISMATCH".to_string(),
                reason: format!(
                    "dao withdrawing output {} stores deposited block number {}, expected {}",
                    input_index, stored_block_number, deposited_block_number
                ),
            });
        }
        Ok(())
    }

    async fn resolve_dao_input_capacity(
        &self,
        consensus: &ConsensusSnapshot,
        current_tx: &TransactionView,
        input_index: usize,
        source: &ResolvedCommittedTransaction,
        source_output_index: usize,
    ) -> std::result::Result<u128, ResolutionIssue> {
        let Some(source_data) = source.tx.inner.outputs_data.get(source_output_index) else {
            return Err(ResolutionIssue {
                status: CheckStatus::Fail,
                error_code: "INPUT_TX_OUTPUT_DATA_MISSING".to_string(),
                reason: format!(
                    "source transaction output data missing at index {}",
                    source_output_index
                ),
            });
        };
        let deposited_block_number = Self::decode_dao_block_number(
            source_data,
            "DAO_INPUT_DATA_INVALID",
            "dao input source data",
        )?;
        if deposited_block_number == 0 {
            let deposited_input_capacity = source
                .tx
                .inner
                .outputs
                .get(source_output_index)
                .map(|output| output.capacity.value())
                .ok_or_else(|| ResolutionIssue {
                    status: CheckStatus::Fail,
                    error_code: "INPUT_TX_OUTPUT_MISSING".to_string(),
                    reason: format!(
                        "source transaction output missing at index {}",
                        source_output_index
                    ),
                })?;
            let source_block_number = source.block_number.ok_or_else(|| ResolutionIssue {
                status: CheckStatus::Unknown,
                error_code: "INPUT_TX_BLOCK_NUMBER_MISSING".to_string(),
                reason: "committed source transaction response missing tx_status.block_number"
                    .to_string(),
            })?;
            self.validate_dao_withdrawing_output(
                consensus,
                current_tx,
                input_index,
                deposited_input_capacity,
                source_block_number,
            )?;
            return Ok(u128::from(deposited_input_capacity));
        }

        let deposit_header_hash = Self::extract_dao_deposit_header_hash(current_tx, input_index)?;
        let deposit_header = self
            .rpc
            .get_header(&deposit_header_hash)
            .await
            .map_err(|err| ResolutionIssue {
                status: CheckStatus::Unknown,
                error_code: "DAO_DEPOSIT_HEADER_RPC_ERROR".to_string(),
                reason: format!(
                    "get_header failed for deposit header {deposit_header_hash:#x}: {err}"
                ),
            })?
            .ok_or_else(|| ResolutionIssue {
                status: CheckStatus::Unknown,
                error_code: "DAO_DEPOSIT_HEADER_MISSING".to_string(),
                reason: format!(
                    "get_header returned null for deposit header {deposit_header_hash:#x}"
                ),
            })?;
        if deposit_header.inner.number.value() != deposited_block_number {
            return Err(ResolutionIssue {
                status: CheckStatus::Fail,
                error_code: "DAO_DEPOSIT_HEADER_BLOCK_NUMBER_MISMATCH".to_string(),
                reason: format!(
                    "dao withdrawing input {} stores deposited block number {}, but referenced header_dep {deposit_header_hash:#x} is block {}",
                    input_index,
                    deposited_block_number,
                    deposit_header.inner.number.value()
                ),
            });
        }
        let Some(deposit_input) = source.tx.inner.inputs.get(source_output_index) else {
            return Err(ResolutionIssue {
                status: CheckStatus::Fail,
                error_code: "DAO_DEPOSIT_REFERENCE_MISSING".to_string(),
                reason: format!(
                    "dao withdrawing source transaction is missing the same-index deposit input {}",
                    source_output_index
                ),
            });
        };
        let deposit_out_point = deposit_input.previous_output.clone();
        let calculation_kind =
            DaoWithdrawingCalculationKind::WithdrawingHeaderHash(source.block_hash.clone());

        match self
            .rpc
            .calculate_dao_maximum_withdraw(deposit_out_point, calculation_kind)
            .await
        {
            Ok(Some(v)) => Ok(v.value() as u128),
            Ok(None) => Err(ResolutionIssue {
                status: CheckStatus::Unknown,
                error_code: "DAO_MAXIMUM_WITHDRAW_MISSING".to_string(),
                reason: format!(
                    "calculate_dao_maximum_withdraw returned null for consensus id '{}' (genesis {:#x})",
                    consensus.consensus_id, consensus.genesis_hash
                ),
            }),
            Err(err) => Err(ResolutionIssue {
                status: CheckStatus::Unknown,
                error_code: "DAO_MAXIMUM_WITHDRAW_RPC_ERROR".to_string(),
                reason: format!("calculate_dao_maximum_withdraw failed: {err}"),
            }),
        }
    }

    async fn audit_transactions(
        &self,
        block: &BlockView,
        core_block: &CoreBlockView,
        log: &mut AuditLog,
        consensus: Option<&ConsensusSnapshot>,
    ) {
        let mut overall_tx_version = CheckStatus::NotApplicable;
        let mut overall_struct = CheckStatus::NotApplicable;
        let mut overall_data_len = CheckStatus::NotApplicable;
        let mut overall_lock_hash_type = CheckStatus::NotApplicable;
        let mut overall_dup_cell_dep = CheckStatus::NotApplicable;
        let mut overall_dup_header_dep = CheckStatus::NotApplicable;
        let mut overall_dup_input_tx = CheckStatus::NotApplicable;
        let mut overall_dup_input_block = CheckStatus::NotApplicable;
        let mut overall_input_resolution = CheckStatus::NotApplicable;
        let mut overall_input_index = CheckStatus::NotApplicable;
        let mut overall_occupied_capacity = CheckStatus::NotApplicable;
        let mut overall_ordinary_capacity = CheckStatus::NotApplicable;
        let mut overall_dao_capacity = CheckStatus::NotApplicable;

        let mut input_cache: HashMap<String, CachedTransaction> = HashMap::new();
        let mut block_inputs = HashSet::new();

        for (tx_index, tx) in block.transactions.iter().enumerate() {
            let packed_tx: packed::Transaction = tx.inner.clone().into();
            if packed_tx.is_cellbase() {
                continue;
            }

            overall_tx_version = merge_status(overall_tx_version, CheckStatus::Pass);
            overall_struct = merge_status(overall_struct, CheckStatus::Pass);
            overall_data_len = merge_status(overall_data_len, CheckStatus::Pass);
            overall_lock_hash_type = merge_status(overall_lock_hash_type, CheckStatus::Pass);
            overall_dup_cell_dep = merge_status(overall_dup_cell_dep, CheckStatus::Pass);
            overall_dup_header_dep = merge_status(overall_dup_header_dep, CheckStatus::Pass);
            overall_dup_input_tx = merge_status(overall_dup_input_tx, CheckStatus::Pass);
            overall_dup_input_block = merge_status(overall_dup_input_block, CheckStatus::Pass);
            overall_input_resolution = merge_status(overall_input_resolution, CheckStatus::Pass);
            overall_input_index = merge_status(overall_input_index, CheckStatus::Pass);
            overall_occupied_capacity = merge_status(overall_occupied_capacity, CheckStatus::Pass);

            if let Some(consensus) = consensus {
                if tx.inner.version.value() != consensus.tx_version {
                    overall_tx_version = merge_status(overall_tx_version, CheckStatus::Fail);
                    log.push_detail(
                        &self.config,
                        DetailItem {
                            check_name: "check_transaction_version".to_string(),
                            status: CheckStatus::Fail,
                            error_code: "TRANSACTION_VERSION_MISMATCH".to_string(),
                            failure_kind: None,
                            rpc_method: None,
                            attempts: None,
                            max_retries: None,
                            tx_hash: Some(format!("{:#x}", tx.hash)),
                            tx_index: Some(tx_index),
                            input_index: None,
                            output_index: None,
                            referenced_out_point: None,
                            expected_operator: Some("equal".to_string()),
                            expected_value: Some(consensus.tx_version.to_string()),
                            actual_value: Some(tx.inner.version.value().to_string()),
                            unit: Some("version".to_string()),
                            reason: "transaction version does not match the consensus tx_version"
                                .to_string(),
                        },
                    );
                }
            } else {
                overall_tx_version = merge_status(overall_tx_version, CheckStatus::Unknown);
            }

            if tx.inner.inputs.is_empty() || tx.inner.outputs.is_empty() {
                overall_struct = merge_status(overall_struct, CheckStatus::Fail);
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_inputs_outputs_structure".to_string(),
                        status: CheckStatus::Fail,
                        error_code: "TRANSACTION_IO_EMPTY".to_string(),
                       failure_kind: None,
                       rpc_method: None,
                       attempts: None,
                       max_retries: None,
                        tx_hash: Some(format!("{:#x}", tx.hash)),
                        tx_index: Some(tx_index),
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: Some("non_empty".to_string()),
                        expected_value: Some("inputs>0 and outputs>0".to_string()),
                        actual_value: Some(format!(
                            "inputs={} outputs={}",
                            tx.inner.inputs.len(),
                            tx.inner.outputs.len()
                        )),
                        unit: None,
                        reason: "non-cellbase transactions must contain at least one input and one output"
                            .to_string(),
                    },
                );
            }

            if tx.inner.outputs.len() != tx.inner.outputs_data.len() {
                overall_data_len = merge_status(overall_data_len, CheckStatus::Fail);
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_outputs_data_length".to_string(),
                        status: CheckStatus::Fail,
                        error_code: "OUTPUTS_DATA_LENGTH_MISMATCH".to_string(),
                        failure_kind: None,
                        rpc_method: None,
                        attempts: None,
                        max_retries: None,
                        tx_hash: Some(format!("{:#x}", tx.hash)),
                        tx_index: Some(tx_index),
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: Some("equal".to_string()),
                        expected_value: Some(tx.inner.outputs.len().to_string()),
                        actual_value: Some(tx.inner.outputs_data.len().to_string()),
                        unit: Some("output".to_string()),
                        reason: "outputs_data length must equal outputs length".to_string(),
                    },
                );
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
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_duplicate_cell_deps".to_string(),
                        status: CheckStatus::Fail,
                        error_code: "DUPLICATE_CELL_DEP".to_string(),
                        failure_kind: None,
                        rpc_method: None,
                        attempts: None,
                        max_retries: None,
                        tx_hash: Some(format!("{:#x}", tx.hash)),
                        tx_index: Some(tx_index),
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: None,
                        expected_value: None,
                        actual_value: None,
                        unit: None,
                        reason: "duplicate cell_dep detected within transaction".to_string(),
                    },
                );
            }

            let mut header_dep_set = HashSet::new();
            if tx
                .inner
                .header_deps
                .iter()
                .any(|h| !header_dep_set.insert(format!("{:#x}", h)))
            {
                overall_dup_header_dep = merge_status(overall_dup_header_dep, CheckStatus::Fail);
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_duplicate_header_deps".to_string(),
                        status: CheckStatus::Fail,
                        error_code: "DUPLICATE_HEADER_DEP".to_string(),
                        failure_kind: None,
                        rpc_method: None,
                        attempts: None,
                        max_retries: None,
                        tx_hash: Some(format!("{:#x}", tx.hash)),
                        tx_index: Some(tx_index),
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: None,
                        expected_value: None,
                        actual_value: None,
                        unit: None,
                        reason: "duplicate header_dep detected within transaction".to_string(),
                    },
                );
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
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_duplicate_inputs_in_transaction".to_string(),
                        status: CheckStatus::Fail,
                        error_code: "DUPLICATE_INPUT_IN_TRANSACTION".to_string(),
                        failure_kind: None,
                        rpc_method: None,
                        attempts: None,
                        max_retries: None,
                        tx_hash: Some(format!("{:#x}", tx.hash)),
                        tx_index: Some(tx_index),
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: None,
                        expected_value: None,
                        actual_value: None,
                        unit: None,
                        reason: "duplicate input out_point detected within transaction".to_string(),
                    },
                );
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
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_duplicate_inputs_in_block".to_string(),
                        status: CheckStatus::Fail,
                        error_code: "DUPLICATE_INPUT_IN_BLOCK".to_string(),
                       failure_kind: None,
                       rpc_method: None,
                       attempts: None,
                       max_retries: None,
                        tx_hash: Some(format!("{:#x}", tx.hash)),
                        tx_index: Some(tx_index),
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: None,
                        expected_value: None,
                        actual_value: None,
                        unit: None,
                        reason: "input out_point was already consumed by another transaction in this block"
                            .to_string(),
                    },
                );
            }

            let mut ordinary_input_sum: u128 = 0;
            let mut ordinary_output_sum: u128 = 0;
            let mut tx_unknown = false;
            let mut classification_unknown = consensus.is_none();
            let mut has_dao_input = false;
            let mut dao_effective_sum: u128 = 0;

            for (output_index, output) in tx.inner.outputs.iter().enumerate() {
                let lock_hash_type_raw: u8 = packed::CellOutput::from(output.clone())
                    .lock()
                    .hash_type()
                    .into();
                if !ckb_types::core::ScriptHashType::verify_value(lock_hash_type_raw) {
                    overall_lock_hash_type =
                        merge_status(overall_lock_hash_type, CheckStatus::Fail);
                    log.push_detail(
                        &self.config,
                        DetailItem {
                            check_name: "check_output_lock_hash_type".to_string(),
                            status: CheckStatus::Fail,
                            error_code: "OUTPUT_LOCK_HASH_TYPE_INVALID".to_string(),
                            failure_kind: None,
                            rpc_method: None,
                            attempts: None,
                            max_retries: None,
                            tx_hash: Some(format!("{:#x}", tx.hash)),
                            tx_index: Some(tx_index),
                            input_index: None,
                            output_index: Some(output_index),
                            referenced_out_point: None,
                            expected_operator: Some("valid_encoding".to_string()),
                            expected_value: Some(
                                "0x01 or any even byte in [0x00,0xfe]".to_string(),
                            ),
                            actual_value: Some(format!("0x{lock_hash_type_raw:02x}")),
                            unit: Some("hash_type".to_string()),
                            reason: "output lock script hash_type encoding is not allowed by CKB consensus"
                                .to_string(),
                        },
                    );
                }
                ordinary_output_sum = match ordinary_output_sum
                    .checked_add(output.capacity.value() as u128)
                {
                    Some(v) => v,
                    None => {
                        overall_ordinary_capacity =
                            merge_status(overall_ordinary_capacity, CheckStatus::Fail);
                        tx_unknown = true;
                        log.push_detail(
                                &self.config,
                                DetailItem {
                                    check_name: "check_ordinary_capacity_conservation".to_string(),
                                    status: CheckStatus::Fail,
                                    error_code: "OUTPUT_CAPACITY_SUM_OVERFLOW".to_string(),
                       failure_kind: None,
                       rpc_method: None,
                       attempts: None,
                       max_retries: None,
                                    tx_hash: Some(format!("{:#x}", tx.hash)),
                                    tx_index: Some(tx_index),
                                    input_index: None,
                                    output_index: Some(output_index),
                                    referenced_out_point: None,
                                    expected_operator: None,
                                    expected_value: None,
                                    actual_value: None,
                                    unit: Some("shannon".to_string()),
                                    reason: "summed output capacity overflowed u128 during ordinary capacity validation"
                                        .to_string(),
                                },
                            );
                        0
                    }
                };
                let data_capacity = tx
                    .inner
                    .outputs_data
                    .get(output_index)
                    .and_then(|data| OccupiedCapacity::bytes(data.len()).ok())
                    .unwrap_or_else(OccupiedCapacity::zero);
                let packed_output = packed::CellOutput::from(output.clone());
                let occupied_capacity = packed_output
                    .occupied_capacity(data_capacity)
                    .ok()
                    .map(|capacity| capacity.as_u64());
                if packed_output
                    .is_lack_of_capacity(data_capacity)
                    .unwrap_or(true)
                {
                    overall_occupied_capacity =
                        merge_status(overall_occupied_capacity, CheckStatus::Fail);
                    log.push_detail(
                        &self.config,
                        DetailItem {
                            check_name: "check_occupied_capacity".to_string(),
                            status: CheckStatus::Fail,
                            error_code: "OUTPUT_BELOW_OCCUPIED_CAPACITY".to_string(),
                            failure_kind: None,
                            rpc_method: None,
                            attempts: None,
                            max_retries: None,
                            tx_hash: Some(format!("{:#x}", tx.hash)),
                            tx_index: Some(tx_index),
                            input_index: None,
                            output_index: Some(output_index),
                            referenced_out_point: None,
                            expected_operator: Some("greater_than_or_equal".to_string()),
                            expected_value: occupied_capacity.map(|value| value.to_string()),
                            actual_value: Some(output.capacity.value().to_string()),
                            unit: Some("shannon".to_string()),
                            reason:
                                "output capacity is below occupied capacity for its data payload"
                                    .to_string(),
                        },
                    );
                }
            }

            for (input_index, input) in tx.inner.inputs.iter().enumerate() {
                let input_tx_hash = input.previous_output.tx_hash.clone();
                let key = format!("{:#x}", input_tx_hash);
                let out_point = format!(
                    "{:#x}:{}",
                    input.previous_output.tx_hash,
                    input.previous_output.index.value()
                );

                if !input_cache.contains_key(&key) {
                    input_cache.insert(
                        key.clone(),
                        self.fetch_committed_transaction(&input_tx_hash).await,
                    );
                }

                let Some(cached) = input_cache.get(&key).cloned() else {
                    continue;
                };
                let resolved = match cached {
                    CachedTransaction::Resolved(resolved) => resolved,
                    CachedTransaction::Unavailable(issue) => {
                        tx_unknown = true;
                        classification_unknown = true;
                        overall_input_resolution =
                            merge_status(overall_input_resolution, issue.status);
                        overall_input_index =
                            merge_status(overall_input_index, CheckStatus::Unknown);
                        self.push_input_detail(
                            log,
                            "check_input_output_index",
                            CheckStatus::Unknown,
                            "INPUT_INDEX_UNVERIFIED",
                            &tx.hash,
                            tx_index,
                            input_index,
                            out_point.clone(),
                            "input output index could not be verified because the source transaction was unavailable"
                                .to_string(),
                        );
                        self.push_input_detail(
                            log,
                            "check_input_content_resolution",
                            issue.status,
                            &issue.error_code,
                            &tx.hash,
                            tx_index,
                            input_index,
                            out_point,
                            issue.reason,
                        );
                        continue;
                    }
                };

                let idx = input.previous_output.index.value() as usize;
                if idx >= resolved.tx.inner.outputs.len() {
                    tx_unknown = true;
                    classification_unknown = true;
                    overall_input_index = merge_status(overall_input_index, CheckStatus::Fail);
                    self.push_input_detail(
                        log,
                        "check_input_output_index",
                        CheckStatus::Fail,
                        "INPUT_OUTPUT_INDEX_OUT_OF_RANGE",
                        &tx.hash,
                        tx_index,
                        input_index,
                        out_point,
                        format!("source transaction output index {} is out of range", idx),
                    );
                    continue;
                }

                let resolved_output = &resolved.tx.inner.outputs[idx];
                let is_dao_input = consensus
                    .map(|consensus| Self::output_uses_dao_type(resolved_output, consensus))
                    .unwrap_or(false);

                if is_dao_input {
                    has_dao_input = true;
                    match self
                        .resolve_dao_input_capacity(
                            consensus.expect("consensus checked above"),
                            tx,
                            input_index,
                            &resolved,
                            idx,
                        )
                        .await
                    {
                        Ok(capacity) => {
                            dao_effective_sum = dao_effective_sum.saturating_add(capacity);
                        }
                        Err(issue) => {
                            tx_unknown = true;
                            classification_unknown = true;
                            overall_dao_capacity = merge_status(overall_dao_capacity, issue.status);
                            self.push_input_detail(
                                log,
                                "check_dao_withdraw_capacity",
                                issue.status,
                                &issue.error_code,
                                &tx.hash,
                                tx_index,
                                input_index,
                                out_point,
                                issue.reason,
                            );
                        }
                    }
                } else if consensus.is_none() {
                    classification_unknown = true;
                    tx_unknown = true;
                } else if resolved.block_hash == H256::default() {
                    tx_unknown = true;
                    classification_unknown = true;
                    overall_input_resolution =
                        merge_status(overall_input_resolution, CheckStatus::Unknown);
                    self.push_input_detail(
                        log,
                        "check_input_content_resolution",
                        CheckStatus::Unknown,
                        "INPUT_SOURCE_BLOCK_HASH_ZERO",
                        &tx.hash,
                        tx_index,
                        input_index,
                        out_point,
                        "committed source transaction returned an all-zero block hash".to_string(),
                    );
                } else {
                    ordinary_input_sum =
                        ordinary_input_sum.saturating_add(resolved_output.capacity.value() as u128);
                }
            }

            if has_dao_input {
                let expected = ordinary_input_sum.saturating_add(dao_effective_sum);
                if tx_unknown {
                    overall_dao_capacity = merge_status(overall_dao_capacity, CheckStatus::Unknown);
                    self.push_unknown_detail(
                        log,
                        "check_dao_withdraw_capacity",
                        "DAO_WITHDRAW_CAPACITY_INCOMPLETE",
                        format!(
                            "dao-related transaction {:#x} has unresolved inputs or prerequisite data",
                            tx.hash
                        ),
                    );
                } else if ordinary_output_sum > expected {
                    overall_dao_capacity = merge_status(overall_dao_capacity, CheckStatus::Fail);
                    log.push_detail(
                        &self.config,
                        DetailItem {
                            check_name: "check_dao_withdraw_capacity".to_string(),
                            status: CheckStatus::Fail,
                            error_code: "DAO_WITHDRAW_CAPACITY_EXCEEDED".to_string(),
                            failure_kind: None,
                            rpc_method: None,
                            attempts: None,
                            max_retries: None,
                            tx_hash: Some(format!("{:#x}", tx.hash)),
                            tx_index: Some(tx_index),
                            input_index: None,
                            output_index: None,
                            referenced_out_point: None,
                            expected_operator: Some("less_than_or_equal".to_string()),
                            expected_value: Some(expected.to_string()),
                            actual_value: Some(ordinary_output_sum.to_string()),
                            unit: Some("shannon".to_string()),
                            reason: "dao effective capacity + ordinary inputs must cover outputs"
                                .to_string(),
                        },
                    );
                } else {
                    overall_dao_capacity = merge_status(overall_dao_capacity, CheckStatus::Pass);
                }
            } else if classification_unknown {
                overall_ordinary_capacity =
                    merge_status(overall_ordinary_capacity, CheckStatus::Unknown);
                overall_dao_capacity = merge_status(overall_dao_capacity, CheckStatus::Unknown);
                self.push_unknown_detail(
                    log,
                    "check_ordinary_capacity_conservation",
                    "ORDINARY_CAPACITY_CLASSIFICATION_UNKNOWN",
                    format!(
                        "transaction {:#x} has unresolved inputs or unknown consensus classification, so ordinary capacity cannot be validated",
                        tx.hash
                    ),
                );
                self.push_unknown_detail(
                    log,
                    "check_dao_withdraw_capacity",
                    "DAO_CLASSIFICATION_UNKNOWN",
                    format!(
                        "transaction {:#x} could not be conclusively classified as ordinary-only or dao-related",
                        tx.hash
                    ),
                );
            } else {
                overall_ordinary_capacity =
                    merge_status(overall_ordinary_capacity, CheckStatus::Pass);
                if tx_unknown {
                    overall_ordinary_capacity =
                        merge_status(overall_ordinary_capacity, CheckStatus::Unknown);
                    self.push_unknown_detail(
                        log,
                        "check_ordinary_capacity_conservation",
                        "ORDINARY_CAPACITY_INCOMPLETE",
                        format!(
                            "ordinary capacity validation for transaction {:#x} was incomplete",
                            tx.hash
                        ),
                    );
                } else if ordinary_output_sum > ordinary_input_sum {
                    overall_ordinary_capacity =
                        merge_status(overall_ordinary_capacity, CheckStatus::Fail);
                    log.push_detail(
                        &self.config,
                        DetailItem {
                            check_name: "check_ordinary_capacity_conservation".to_string(),
                            status: CheckStatus::Fail,
                            error_code: "OUTPUT_CAPACITY_EXCEEDS_INPUT".to_string(),
                            failure_kind: None,
                            rpc_method: None,
                            attempts: None,
                            max_retries: None,
                            tx_hash: Some(format!("{:#x}", tx.hash)),
                            tx_index: Some(tx_index),
                            input_index: None,
                            output_index: None,
                            referenced_out_point: None,
                            expected_operator: Some("less_than_or_equal".to_string()),
                            expected_value: Some(ordinary_input_sum.to_string()),
                            actual_value: Some(ordinary_output_sum.to_string()),
                            unit: Some("shannon".to_string()),
                            reason: "ordinary outputs must be <= ordinary inputs".to_string(),
                        },
                    );
                }
            }
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
        log.check_dao_withdraw_capacity = overall_dao_capacity;
    }

    async fn audit_reward(
        &self,
        block: &BlockView,
        log: &mut AuditLog,
        consensus: Option<&ConsensusSnapshot>,
    ) {
        let Some(cellbase) = block.transactions.first() else {
            log.check_cellbase_reward_amount = CheckStatus::Fail;
            log.check_cellbase_reward_target = CheckStatus::Unknown;
            log.push_detail(
                &self.config,
                DetailItem {
                    check_name: "check_cellbase_reward_amount".to_string(),
                    status: CheckStatus::Fail,
                    error_code: "CELLBASE_MISSING".to_string(),
                    failure_kind: None,
                    rpc_method: None,
                    attempts: None,
                    max_retries: None,
                    tx_hash: None,
                    tx_index: None,
                    input_index: None,
                    output_index: None,
                    referenced_out_point: None,
                    expected_operator: None,
                    expected_value: None,
                    actual_value: None,
                    unit: None,
                    reason: "block is missing a cellbase transaction".to_string(),
                },
            );
            self.push_unknown_detail(
                log,
                "check_cellbase_reward_target",
                "CELLBASE_MISSING",
                "reward target cannot be verified without a cellbase transaction".to_string(),
            );
            return;
        };

        let actual = cellbase.inner.outputs.iter().fold(0u128, |acc, o| {
            acc.saturating_add(o.capacity.value() as u128)
        });

        let Some(consensus) = consensus else {
            log.check_cellbase_reward_amount = CheckStatus::Unknown;
            log.check_cellbase_reward_target = CheckStatus::Unknown;
            self.push_unknown_detail(
                log,
                "check_cellbase_reward_amount",
                "CONSENSUS_UNAVAILABLE",
                "reward validation requires get_consensus parameters".to_string(),
            );
            self.push_unknown_detail(
                log,
                "check_cellbase_reward_target",
                "CONSENSUS_UNAVAILABLE",
                "reward target validation requires get_consensus parameters".to_string(),
            );
            return;
        };

        let block_number = block.header.inner.number.value();
        if block_number <= consensus.finalization_delay_length {
            let status = if cellbase.inner.outputs.is_empty() {
                CheckStatus::Pass
            } else {
                CheckStatus::Fail
            };
            log.check_cellbase_reward_amount = status;
            log.check_cellbase_reward_target = status;
            if status == CheckStatus::Fail {
                log.push_detail(
                    &self.config,
                    DetailItem {
                        check_name: "check_cellbase_reward_target".to_string(),
                        status,
                        error_code: "REWARD_FINALIZATION_NOT_READY".to_string(),
                       failure_kind: None,
                       rpc_method: None,
                       attempts: None,
                       max_retries: None,
                        tx_hash: Some(format!("{:#x}", cellbase.hash)),
                        tx_index: Some(0),
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: Some("equal".to_string()),
                        expected_value: Some("0".to_string()),
                        actual_value: Some(actual.to_string()),
                        unit: Some("shannon".to_string()),
                        reason: "block has no finalized reward target yet, so cellbase outputs must be empty"
                            .to_string(),
                    },
                );
            }
            return;
        }

        let target_number = block_number - consensus.finalization_delay_length;
        let target_header = match self.rpc.get_header_by_number(target_number).await {
            Ok(Some(header)) => {
                self.cache_header(&header);
                header
            }
            Ok(None) => {
                log.check_cellbase_reward_amount = CheckStatus::Unknown;
                log.check_cellbase_reward_target = CheckStatus::Unknown;
                self.push_unknown_detail(
                    log,
                    "check_cellbase_reward_amount",
                    "REWARD_TARGET_HEADER_MISSING",
                    format!("target block header missing at height {}", target_number),
                );
                return;
            }
            Err(err) => {
                log.check_cellbase_reward_amount = CheckStatus::Unknown;
                log.check_cellbase_reward_target = CheckStatus::Unknown;
                self.push_unknown_detail(
                    log,
                    "check_cellbase_reward_amount",
                    "REWARD_TARGET_HEADER_RPC_ERROR",
                    format!("target block header lookup failed: {err}"),
                );
                return;
            }
        };
        let target_hash = target_header.hash;

        match self.rpc.get_block_economic_state(&target_hash).await {
            Ok(Some(economic)) => {
                let expected = economic.miner_reward.primary.value() as u128
                    + economic.miner_reward.secondary.value() as u128
                    + economic.miner_reward.committed.value() as u128
                    + economic.miner_reward.proposal.value() as u128;

                if economic.finalized_at != block.header.hash {
                    log.check_cellbase_reward_amount = CheckStatus::Unknown;
                    log.check_cellbase_reward_target = CheckStatus::Unknown;
                    self.push_unknown_detail(
                        log,
                        "check_cellbase_reward_amount",
                        "REWARD_FINALIZATION_MISMATCH",
                        format!(
                            "economic state finalized_at {:#x} does not match audited block hash {:#x}",
                            economic.finalized_at, block.header.hash
                        ),
                    );
                    self.push_unknown_detail(
                        log,
                        "check_cellbase_reward_target",
                        "REWARD_FINALIZATION_MISMATCH",
                        format!(
                            "cannot validate reward target because economic state finalized_at {:#x} does not match audited block hash {:#x}",
                            economic.finalized_at, block.header.hash
                        ),
                    );
                    return;
                }

                let target_block = match self.get_block_cached(&target_hash).await {
                    Ok(Some(target_block)) if target_block.header.hash == target_hash => {
                        target_block
                    }
                    Ok(Some(_)) => {
                        log.check_cellbase_reward_amount = CheckStatus::Unknown;
                        log.check_cellbase_reward_target = CheckStatus::Unknown;
                        self.push_unknown_detail(
                            log,
                            "check_cellbase_reward_amount",
                            "REWARD_TARGET_BLOCK_MISMATCH",
                            "reward amount cannot be validated because the target block fetched by hash did not match the requested hash".to_string(),
                        );
                        self.push_unknown_detail(
                            log,
                            "check_cellbase_reward_target",
                            "REWARD_TARGET_BLOCK_MISMATCH",
                            "target block fetched by hash did not match requested hash".to_string(),
                        );
                        return;
                    }
                    Ok(None) => {
                        log.check_cellbase_reward_amount = CheckStatus::Unknown;
                        log.check_cellbase_reward_target = CheckStatus::Unknown;
                        self.push_unknown_detail(
                            log,
                            "check_cellbase_reward_amount",
                            "REWARD_TARGET_BLOCK_MISSING",
                            format!("reward amount cannot be validated because target block {} is unavailable by hash", target_number),
                        );
                        self.push_unknown_detail(
                            log,
                            "check_cellbase_reward_target",
                            "REWARD_TARGET_BLOCK_MISSING",
                            format!("target block {} unavailable by hash", target_number),
                        );
                        return;
                    }
                    Err(err) => {
                        log.check_cellbase_reward_amount = CheckStatus::Unknown;
                        log.check_cellbase_reward_target = CheckStatus::Unknown;
                        self.push_unknown_detail(
                            log,
                            "check_cellbase_reward_amount",
                            "REWARD_TARGET_BLOCK_RPC_ERROR",
                            format!("reward amount cannot be validated because target block lookup failed: {err}"),
                        );
                        self.push_unknown_detail(
                            log,
                            "check_cellbase_reward_target",
                            "REWARD_TARGET_BLOCK_RPC_ERROR",
                            format!("target block lookup failed: {err}"),
                        );
                        return;
                    }
                };
                let Some(target_cellbase) = target_block.transactions.first() else {
                    log.check_cellbase_reward_amount = CheckStatus::Unknown;
                    log.check_cellbase_reward_target = CheckStatus::Unknown;
                    self.push_unknown_detail(
                        log,
                        "check_cellbase_reward_amount",
                        "REWARD_TARGET_CELLBASE_MISSING",
                        "reward amount cannot be validated because the target block is missing cellbase transaction".to_string(),
                    );
                    self.push_unknown_detail(
                        log,
                        "check_cellbase_reward_target",
                        "REWARD_TARGET_CELLBASE_MISSING",
                        "target block is missing cellbase transaction".to_string(),
                    );
                    return;
                };
                let Some(target_lock) = target_cellbase.inner.witnesses.first().and_then(|w| {
                    packed::CellbaseWitness::from_slice(w.as_bytes())
                        .ok()
                        .map(|x| x.lock())
                }) else {
                    log.check_cellbase_reward_amount = CheckStatus::Unknown;
                    log.check_cellbase_reward_target = CheckStatus::Unknown;
                    self.push_unknown_detail(
                        log,
                        "check_cellbase_reward_amount",
                        "REWARD_TARGET_WITNESS_INVALID",
                        "reward amount cannot be validated because the target cellbase witness cannot be decoded".to_string(),
                    );
                    self.push_unknown_detail(
                        log,
                        "check_cellbase_reward_target",
                        "REWARD_TARGET_WITNESS_INVALID",
                        "target cellbase witness cannot be decoded".to_string(),
                    );
                    return;
                };
                let expected_lock = ckb_jsonrpc_types::Script::from(target_lock.clone());
                let insufficient_reward = packed::CellOutput::new_builder()
                    .capacity(expected as u64)
                    .lock(target_lock)
                    .build()
                    .is_lack_of_capacity(OccupiedCapacity::zero())
                    .unwrap_or(true);

                if insufficient_reward {
                    let status = if cellbase.inner.outputs.is_empty() {
                        CheckStatus::Pass
                    } else {
                        CheckStatus::Fail
                    };
                    log.check_cellbase_reward_amount = status;
                    log.check_cellbase_reward_target = status;
                    if status == CheckStatus::Fail {
                        log.push_detail(
                            &self.config,
                            DetailItem {
                                check_name: "check_cellbase_reward_target".to_string(),
                                status,
                                error_code: "REWARD_BELOW_OCCUPIED_CAPACITY".to_string(),
                       failure_kind: None,
                       rpc_method: None,
                       attempts: None,
                       max_retries: None,
                                tx_hash: Some(format!("{:#x}", cellbase.hash)),
                                tx_index: Some(0),
                                input_index: None,
                                output_index: None,
                                referenced_out_point: None,
                                expected_operator: Some("equal".to_string()),
                                expected_value: Some(format!("0 (target_block={target_number} target_hash={target_hash:#x})")),
                                actual_value: Some(actual.to_string()),
                                unit: Some("shannon".to_string()),
                                reason: "finalized reward is below the occupied capacity of the correct target lock, so cellbase outputs must be empty".to_string(),
                            },
                        );
                    }
                    return;
                }

                if actual == expected {
                    log.check_cellbase_reward_amount = CheckStatus::Pass;
                } else {
                    log.check_cellbase_reward_amount = CheckStatus::Fail;
                    log.push_detail(
                        &self.config,
                        DetailItem {
                            check_name: "check_cellbase_reward_amount".to_string(),
                            status: CheckStatus::Fail,
                            error_code: "CELLBASE_REWARD_MISMATCH".to_string(),
                       failure_kind: None,
                       rpc_method: None,
                       attempts: None,
                       max_retries: None,
                            tx_hash: Some(format!("{:#x}", cellbase.hash)),
                            tx_index: Some(0),
                            input_index: None,
                            output_index: None,
                            referenced_out_point: None,
                            expected_operator: Some("equal".to_string()),
                            expected_value: Some(format!("{expected} (target_block={target_number} target_hash={target_hash:#x})")),
                            actual_value: Some(actual.to_string()),
                            unit: Some("shannon".to_string()),
                            reason: "cellbase output total does not match finalized target block economic state"
                                .to_string(),
                        },
                    );
                }

                if cellbase.inner.outputs.is_empty() {
                    log.check_cellbase_reward_target = CheckStatus::Fail;
                    log.push_detail(
                        &self.config,
                        DetailItem {
                            check_name: "check_cellbase_reward_target".to_string(),
                            status: CheckStatus::Fail,
                            error_code: "CELLBASE_REWARD_TARGET_MISSING".to_string(),
                       failure_kind: None,
                       rpc_method: None,
                       attempts: None,
                       max_retries: None,
                            tx_hash: Some(format!("{:#x}", cellbase.hash)),
                            tx_index: Some(0),
                            input_index: None,
                            output_index: None,
                            referenced_out_point: None,
                            expected_operator: Some("equal".to_string()),
                            expected_value: None,
                            actual_value: None,
                            unit: None,
                            reason: format!(
                                "cellbase must include an output paying the finalized target block {} recipient",
                                target_number
                            ),
                        },
                    );
                } else {
                    let paid_to_expected_lock = cellbase
                        .inner
                        .outputs
                        .iter()
                        .filter(|output| output.lock == expected_lock)
                        .fold(0u128, |sum, output| {
                            sum.saturating_add(output.capacity.value() as u128)
                        });

                    if paid_to_expected_lock == expected {
                        log.check_cellbase_reward_target = CheckStatus::Pass;
                    } else {
                        log.check_cellbase_reward_target = CheckStatus::Fail;
                        log.push_detail(
                            &self.config,
                            DetailItem {
                                check_name: "check_cellbase_reward_target".to_string(),
                                status: CheckStatus::Fail,
                                error_code: "CELLBASE_REWARD_TARGET_MISMATCH".to_string(),
                       failure_kind: None,
                       rpc_method: None,
                       attempts: None,
                       max_retries: None,
                                tx_hash: Some(format!("{:#x}", cellbase.hash)),
                                tx_index: Some(0),
                                input_index: None,
                                output_index: Some(0),
                                referenced_out_point: None,
                                expected_operator: Some("equal".to_string()),
                                expected_value: Some(format!("{expected} (target_block={target_number} target_hash={target_hash:#x})")),
                                actual_value: Some(paid_to_expected_lock.to_string()),
                                unit: Some("shannon".to_string()),
                                reason: format!(
                                    "cellbase outputs locked to target block {} recipient do not carry the full finalized reward",
                                    target_number
                                ),
                            },
                        );
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
                       failure_kind: None,
                       rpc_method: None,
                       attempts: None,
                       max_retries: None,
                        tx_hash: None,
                        tx_index: None,
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: None,
                        expected_value: None,
                        actual_value: None,
                        unit: None,
                        reason: format!(
                            "get_block_economic_state returned null for reward target block {} ({target_hash:#x})",
                            target_number
                        ),
                    },
                );
                self.push_unknown_detail(
                    log,
                    "check_cellbase_reward_target",
                    "ECONOMIC_STATE_MISSING",
                    format!(
                        "reward target cannot be fully validated because get_block_economic_state returned null for reward target block {} ({target_hash:#x})",
                        target_number
                    ),
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
                       failure_kind: None,
                       rpc_method: None,
                       attempts: None,
                       max_retries: None,
                        tx_hash: None,
                        tx_index: None,
                        input_index: None,
                        output_index: None,
                        referenced_out_point: None,
                        expected_operator: None,
                        expected_value: None,
                        actual_value: None,
                        unit: None,
                        reason: format!(
                            "get_block_economic_state failed for reward target block {} ({target_hash:#x}): {err}",
                            target_number
                        ),
                    },
                );
                self.push_unknown_detail(
                    log,
                    "check_cellbase_reward_target",
                    "ECONOMIC_STATE_RPC_ERROR",
                    format!(
                        "reward target cannot be fully validated because get_block_economic_state failed for reward target block {} ({target_hash:#x}): {err}",
                        target_number
                    ),
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
    };
    if rank(new_status) > rank(old) {
        new_status
    } else {
        old
    }
}

fn sanitize_rate_limit_reason(reason: &str) -> String {
    reason
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(240)
        .collect()
}

fn sanitize_diagnostic_reason(reason: &str) -> String {
    reason
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(320)
        .collect()
}

fn format_reason_suffix(reason: &str) -> String {
    if reason.is_empty() {
        String::new()
    } else {
        format!(" reason={reason}")
    }
}

fn format_http_status_error(
    method: &str,
    status: StatusCode,
    body: &str,
    http_attempts: u32,
) -> String {
    let suffix = if body.is_empty() {
        String::new()
    } else {
        format!(": {}", body)
    };
    format!(
        "rpc {} http status {} after {} http attempt(s){}",
        method, status, http_attempts, suffix
    )
}

fn parse_retry_after_value(value: &str, now: DateTime<Utc>) -> Option<std::time::Duration> {
    let trimmed = value.trim();
    if let Ok(seconds) = trimmed.parse::<u64>() {
        return Some(std::time::Duration::from_secs(seconds));
    }
    let retry_at = chrono::DateTime::parse_from_rfc2822(trimmed)
        .ok()?
        .with_timezone(&Utc);
    if retry_at <= now {
        return None;
    }
    (retry_at - now).to_std().ok()
}

fn parse_retry_after_from_body(body: &str) -> Option<std::time::Duration> {
    let lower = body.to_ascii_lowercase();
    let anchor = lower.find("try again after ")?;
    let digits: String = lower[anchor + "try again after ".len()..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();
    let seconds = digits.parse::<u64>().ok()?;
    Some(std::time::Duration::from_secs(seconds))
}

fn retry_backoff(attempt: u32) -> std::time::Duration {
    let factor = 1u64 << attempt.saturating_sub(1).min(6);
    std::time::Duration::from_millis(RETRY_BASE_DELAY_MS.saturating_mul(factor))
}

fn is_retryable_execution_error(error_code: &str) -> bool {
    matches!(
        error_code,
        "PARENT_HEADER_MISSING"
            | "PARENT_HEADER_UNAVAILABLE"
            | "TIMESTAMP_ANCESTOR_UNAVAILABLE"
            | "TIMESTAMP_MEDIAN_INCOMPLETE"
            | "INPUT_TX_NOT_COMMITTED"
            | "INPUT_TX_BLOCK_HASH_MISSING"
            | "INPUT_TX_JSON_MISSING"
            | "INPUT_TX_MISSING"
            | "INPUT_TX_RPC_ERROR"
            | "INPUT_TX_BLOCK_NUMBER_MISSING"
            | "INPUT_INDEX_UNVERIFIED"
            | "INPUT_SOURCE_BLOCK_HASH_ZERO"
            | "ORDINARY_CAPACITY_CLASSIFICATION_UNKNOWN"
            | "ORDINARY_CAPACITY_INCOMPLETE"
            | "DAO_WITHDRAW_CAPACITY_INCOMPLETE"
            | "DAO_CLASSIFICATION_UNKNOWN"
            | "DAO_DEPOSIT_HEADER_MISSING"
            | "DAO_DEPOSIT_HEADER_RPC_ERROR"
            | "REWARD_TARGET_HEADER_MISSING"
            | "REWARD_TARGET_HEADER_RPC_ERROR"
            | "REWARD_FINALIZATION_MISMATCH"
            | "REWARD_TARGET_BLOCK_MISMATCH"
            | "REWARD_TARGET_BLOCK_MISSING"
            | "REWARD_TARGET_BLOCK_RPC_ERROR"
            | "REWARD_TARGET_CELLBASE_MISSING"
            | "REWARD_TARGET_WITNESS_INVALID"
            | "ECONOMIC_STATE_MISSING"
            | "ECONOMIC_STATE_RPC_ERROR"
            | "DAO_MAXIMUM_WITHDRAW_MISSING"
            | "DAO_MAXIMUM_WITHDRAW_RPC_ERROR"
            | "CONSENSUS_UNAVAILABLE"
    )
}

fn is_block_retryable_execution_error(error_code: &str) -> bool {
    matches!(
        error_code,
        "PARENT_HEADER_MISSING"
            | "TIMESTAMP_MEDIAN_INCOMPLETE"
            | "INPUT_TX_NOT_COMMITTED"
            | "INPUT_TX_BLOCK_HASH_MISSING"
            | "INPUT_TX_JSON_MISSING"
            | "INPUT_TX_MISSING"
            | "INPUT_TX_BLOCK_NUMBER_MISSING"
            | "INPUT_INDEX_UNVERIFIED"
            | "INPUT_SOURCE_BLOCK_HASH_ZERO"
            | "ORDINARY_CAPACITY_CLASSIFICATION_UNKNOWN"
            | "ORDINARY_CAPACITY_INCOMPLETE"
            | "DAO_WITHDRAW_CAPACITY_INCOMPLETE"
            | "DAO_CLASSIFICATION_UNKNOWN"
            | "DAO_DEPOSIT_HEADER_MISSING"
            | "REWARD_TARGET_HEADER_MISSING"
            | "REWARD_FINALIZATION_MISMATCH"
            | "REWARD_TARGET_BLOCK_MISMATCH"
            | "REWARD_TARGET_BLOCK_MISSING"
            | "REWARD_TARGET_CELLBASE_MISSING"
            | "REWARD_TARGET_WITNESS_INVALID"
            | "ECONOMIC_STATE_MISSING"
            | "DAO_MAXIMUM_WITHDRAW_MISSING"
    )
}

fn rpc_method_for_error_code(error_code: &str) -> Option<&'static str> {
    match error_code {
        "PARENT_HEADER_MISSING"
        | "PARENT_HEADER_UNAVAILABLE"
        | "TIMESTAMP_ANCESTOR_UNAVAILABLE" => Some("get_header"),
        "INPUT_TX_NOT_COMMITTED"
        | "INPUT_TX_BLOCK_HASH_MISSING"
        | "INPUT_TX_JSON_MISSING"
        | "INPUT_TX_MISSING"
        | "INPUT_TX_RPC_ERROR"
        | "INPUT_TX_BLOCK_NUMBER_MISSING"
        | "INPUT_INDEX_UNVERIFIED"
        | "INPUT_SOURCE_BLOCK_HASH_ZERO" => Some("get_transaction"),
        "DAO_DEPOSIT_HEADER_MISSING" | "DAO_DEPOSIT_HEADER_RPC_ERROR" => Some("get_header"),
        "REWARD_TARGET_HEADER_MISSING" | "REWARD_TARGET_HEADER_RPC_ERROR" => {
            Some("get_header_by_number")
        }
        "REWARD_FINALIZATION_MISMATCH" | "ECONOMIC_STATE_MISSING" | "ECONOMIC_STATE_RPC_ERROR" => {
            Some("get_block_economic_state")
        }
        "REWARD_TARGET_BLOCK_MISMATCH"
        | "REWARD_TARGET_BLOCK_MISSING"
        | "REWARD_TARGET_BLOCK_RPC_ERROR"
        | "REWARD_TARGET_CELLBASE_MISSING"
        | "REWARD_TARGET_WITNESS_INVALID" => Some("get_block"),
        "DAO_MAXIMUM_WITHDRAW_MISSING" | "DAO_MAXIMUM_WITHDRAW_RPC_ERROR" => {
            Some("calculate_dao_maximum_withdraw")
        }
        _ => None,
    }
}

fn epoch_continuity(parent_value: u64, current_value: u64) -> (bool, String) {
    let parent = EpochNumberWithFraction::from_full_value(parent_value);
    let current = EpochNumberWithFraction::from_full_value(current_value);

    if !current.is_well_formed() {
        return (
            false,
            format!(
                "invalid current epoch format parent={}:{}:{} current={}:{}:{}",
                parent.number(),
                parent.index(),
                parent.length(),
                current.number(),
                current.index(),
                current.length()
            ),
        );
    }

    if parent.is_genesis() || current.is_successor_of(parent) {
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
    use std::collections::VecDeque;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use tokio::sync::Notify;

    #[derive(Default)]
    struct MockRpc {
        tip: Mutex<Option<HeaderView>>,
        headers_by_number: Mutex<HashMap<u64, HeaderView>>,
        headers_by_hash: Mutex<HashMap<String, HeaderView>>,
        blocks_by_number: Mutex<HashMap<u64, BlockView>>,
        blocks_by_hash: Mutex<HashMap<String, BlockView>>,
        txs: Mutex<HashMap<String, serde_json::Value>>,
        economics: Mutex<HashMap<String, BlockEconomicState>>,
        dao_max: Mutex<HashMap<String, Uint64>>,
        dao_errors: Mutex<HashMap<String, String>>,
        dao_calls: Mutex<Vec<(String, String)>>,
        consensus: Mutex<Option<RpcConsensus>>,
        method_calls: Mutex<HashMap<String, u64>>,
    }

    impl MockRpc {
        fn bump(&self, method: &str) {
            let mut calls = self.method_calls.lock().unwrap();
            let entry = calls.entry(method.to_string()).or_default();
            *entry = entry.saturating_add(1);
        }

        fn method_calls(&self, method: &str) -> u64 {
            self.method_calls
                .lock()
                .unwrap()
                .get(method)
                .copied()
                .unwrap_or(0)
        }
    }

    struct ManualRpcClock {
        now: StdMutex<std::time::Duration>,
        base_utc: DateTime<Utc>,
        notify: Notify,
    }

    impl ManualRpcClock {
        fn new(base_utc: DateTime<Utc>) -> Self {
            Self {
                now: StdMutex::new(std::time::Duration::ZERO),
                base_utc,
                notify: Notify::new(),
            }
        }

        fn advance(&self, delay: std::time::Duration) {
            let mut now = self.now.lock().unwrap();
            *now += delay;
            self.notify.notify_waiters();
        }
    }

    #[async_trait]
    impl RpcClock for ManualRpcClock {
        fn now(&self) -> std::time::Duration {
            *self.now.lock().unwrap()
        }

        fn now_utc(&self) -> DateTime<Utc> {
            chrono::Duration::from_std(*self.now.lock().unwrap())
                .ok()
                .map(|delta| self.base_utc + delta)
                .unwrap_or(self.base_utc)
        }

        async fn sleep(
            &self,
            delay: std::time::Duration,
            shutdown: &CancellationToken,
            context: &'static str,
        ) -> Result<()> {
            let target = self.now() + delay;
            loop {
                if self.now() >= target {
                    return Ok(());
                }
                tokio::select! {
                    _ = self.notify.notified() => {}
                    _ = shutdown.cancelled() => return Err(shutdown_error(context)),
                }
            }
        }
    }

    #[async_trait]
    impl CkbRpc for MockRpc {
        async fn get_tip_header(&self) -> Result<Option<HeaderView>> {
            self.bump("get_tip_header");
            Ok(self.tip.lock().unwrap().clone())
        }
        async fn get_block(&self, hash: &H256) -> Result<Option<BlockView>> {
            self.bump("get_block");
            Ok(self
                .blocks_by_hash
                .lock()
                .unwrap()
                .get(&format!("{hash:#x}"))
                .cloned())
        }
        async fn get_header_by_number(&self, number: u64) -> Result<Option<HeaderView>> {
            self.bump("get_header_by_number");
            Ok(self.headers_by_number.lock().unwrap().get(&number).cloned())
        }
        async fn get_header(&self, hash: &H256) -> Result<Option<HeaderView>> {
            self.bump("get_header");
            Ok(self
                .headers_by_hash
                .lock()
                .unwrap()
                .get(&format!("{hash:#x}"))
                .cloned())
        }
        async fn get_block_by_number(&self, number: u64) -> Result<Option<BlockView>> {
            self.bump("get_block_by_number");
            Ok(self.blocks_by_number.lock().unwrap().get(&number).cloned())
        }
        async fn get_transaction(
            &self,
            hash: &H256,
        ) -> Result<Option<TransactionWithStatusResponse>> {
            self.bump("get_transaction");
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
            self.bump("get_block_economic_state");
            Ok(self
                .economics
                .lock()
                .unwrap()
                .get(&format!("{hash:#x}"))
                .cloned())
        }
        async fn get_consensus(&self) -> Result<RpcConsensus> {
            self.bump("get_consensus");
            self.consensus
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| anyhow!("mock consensus missing"))
        }
        async fn calculate_dao_maximum_withdraw(
            &self,
            out_point: OutPoint,
            kind: DaoWithdrawingCalculationKind,
        ) -> Result<Option<Uint64>> {
            self.bump("calculate_dao_maximum_withdraw");
            let out_point_key = format!("{:#x}:{}", out_point.tx_hash, out_point.index.value());
            let kind_key = match &kind {
                DaoWithdrawingCalculationKind::WithdrawingHeaderHash(hash) => {
                    format!("header:{hash:#x}")
                }
                DaoWithdrawingCalculationKind::WithdrawingOutPoint(out_point) => format!(
                    "out_point:{:#x}:{}",
                    out_point.tx_hash,
                    out_point.index.value()
                ),
            };
            self.dao_calls
                .lock()
                .unwrap()
                .push((out_point_key.clone(), kind_key));
            if let Some(err) = self.dao_errors.lock().unwrap().get(&out_point_key).cloned() {
                return Err(anyhow!(err));
            }
            Ok(self.dao_max.lock().unwrap().get(&out_point_key).cloned())
        }
    }

    #[derive(Default)]
    struct RetryRpc {
        base: MockRpc,
        tx_sequence: Mutex<HashMap<String, VecDeque<Option<serde_json::Value>>>>,
        tx_call_count: Mutex<HashMap<String, usize>>,
    }

    impl RetryRpc {
        fn queue_tx_sequence(&self, hash: &H256, values: Vec<Option<serde_json::Value>>) {
            self.tx_sequence
                .lock()
                .unwrap()
                .insert(format!("{hash:#x}"), VecDeque::from(values));
        }

        fn tx_calls(&self, hash: &H256) -> usize {
            self.tx_call_count
                .lock()
                .unwrap()
                .get(&format!("{hash:#x}"))
                .copied()
                .unwrap_or(0)
        }
    }

    #[async_trait]
    impl CkbRpc for RetryRpc {
        async fn get_tip_header(&self) -> Result<Option<HeaderView>> {
            Ok(self.base.tip.lock().unwrap().clone())
        }

        async fn get_block(&self, hash: &H256) -> Result<Option<BlockView>> {
            Ok(self
                .base
                .blocks_by_hash
                .lock()
                .unwrap()
                .get(&format!("{hash:#x}"))
                .cloned())
        }

        async fn get_header_by_number(&self, number: u64) -> Result<Option<HeaderView>> {
            Ok(self
                .base
                .headers_by_number
                .lock()
                .unwrap()
                .get(&number)
                .cloned())
        }

        async fn get_header(&self, hash: &H256) -> Result<Option<HeaderView>> {
            Ok(self
                .base
                .headers_by_hash
                .lock()
                .unwrap()
                .get(&format!("{hash:#x}"))
                .cloned())
        }

        async fn get_block_by_number(&self, number: u64) -> Result<Option<BlockView>> {
            Ok(self
                .base
                .blocks_by_number
                .lock()
                .unwrap()
                .get(&number)
                .cloned())
        }

        async fn get_transaction(
            &self,
            hash: &H256,
        ) -> Result<Option<TransactionWithStatusResponse>> {
            let key = format!("{hash:#x}");
            *self
                .tx_call_count
                .lock()
                .unwrap()
                .entry(key.clone())
                .or_default() += 1;
            if let Some(values) = self.tx_sequence.lock().unwrap().get_mut(&key)
                && let Some(value) = values.pop_front()
            {
                return value
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(Into::into);
            }
            Ok(self
                .base
                .txs
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .map(serde_json::from_value)
                .transpose()?)
        }

        async fn get_block_economic_state(
            &self,
            hash: &H256,
        ) -> Result<Option<BlockEconomicState>> {
            Ok(self
                .base
                .economics
                .lock()
                .unwrap()
                .get(&format!("{hash:#x}"))
                .cloned())
        }

        async fn get_consensus(&self) -> Result<RpcConsensus> {
            self.base
                .consensus
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| anyhow!("mock consensus missing"))
        }

        async fn calculate_dao_maximum_withdraw(
            &self,
            out_point: OutPoint,
            _kind: DaoWithdrawingCalculationKind,
        ) -> Result<Option<Uint64>> {
            Ok(self
                .base
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
            genesis_hash: Some(format!("{:#x}", H256::from([1u8; 32]))),
            last_height: 1,
            last_hash: "0x1".to_string(),
            next_height: None,
            next_hash: None,
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
            node_id: "node-1".to_string(),
            poll_interval_ms: 1000,
            rpc_timeout_secs: 10,
            max_retries: 0,
            cursor_path: Some(cursor_path),
            log_path: None,
            max_details: 100,
            max_future_ms: 15_000,
            median_time_span: 11,
            proposal_limit: 1500,
            tx_version: 0,
            history_retention: 32,
            dao_type_hash: String::new(),
            header_cache_capacity: 8192,
            block_cache_entries: 64,
            block_cache_max_bytes: 64 * 1024 * 1024,
            stats_interval_secs: 60,
        }
    }

    fn mock_consensus() -> RpcConsensus {
        mock_consensus_with_id_and_limit("testnet", H256::from([1u8; 32]), 0x1000)
    }

    fn mock_consensus_with_limit(max_block_bytes: u64) -> RpcConsensus {
        mock_consensus_with_id_and_limit("testnet", H256::from([1u8; 32]), max_block_bytes)
    }

    fn mock_consensus_with_id_and_limit(
        id: &str,
        genesis_hash: H256,
        max_block_bytes: u64,
    ) -> RpcConsensus {
        serde_json::from_value(json!({
            "id": id,
            "genesis_hash": format!("{genesis_hash:#x}"),
            "dao_type_hash": format!("{:#x}", H256::from([2u8; 32])),
            "secp256k1_blake160_sighash_all_type_hash": null,
            "secp256k1_blake160_multisig_all_type_hash": null,
            "initial_primary_epoch_reward": "0x0",
            "secondary_epoch_reward": "0x0",
            "max_uncles_num": "0x2",
            "orphan_rate_target": {"numer": "0x1", "denom": "0x2"},
            "epoch_duration_target": "0x1",
            "tx_proposal_window": {"closest": "0x2", "farthest": "0xa"},
            "proposer_reward_ratio": {"numer": "0x2", "denom": "0x5"},
            "cellbase_maturity": "0x0",
            "median_time_block_count": "0xb",
            "max_block_cycles": "0x1",
            "max_block_bytes": format!("0x{max_block_bytes:x}"),
            "block_version": "0x0",
            "tx_version": "0x0",
            "type_id_code_hash": format!("{:#x}", H256::from([3u8; 32])),
            "max_block_proposals_limit": "0x5dc",
            "primary_epoch_reward_halving_interval": "0x1",
            "permanent_difficulty_in_dummy": false,
            "hardfork_features": [],
            "softforks": {}
        }))
        .unwrap()
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

    fn dao_type_script(consensus: &ConsensusSnapshot) -> ckb_types::packed::Script {
        ckb_types::packed::Script::new_builder()
            .code_hash(consensus.dao_type_hash.pack())
            .hash_type(ckb_types::core::ScriptHashType::Type)
            .args(ckb_types::bytes::Bytes::new())
            .build()
    }

    fn dao_cell_output(consensus: &ConsensusSnapshot, cap: u64) -> CellOutput {
        CellOutput::new_builder()
            .capacity(cap)
            .lock(simple_lock_script())
            .type_(
                ScriptOpt::new_builder()
                    .set(Some(dao_type_script(consensus)))
                    .build(),
            )
            .build()
    }

    fn le_u64_bytes(value: u64) -> ckb_types::bytes::Bytes {
        value.to_le_bytes().to_vec().into()
    }

    fn witness_with_input_type_u64(value: u64) -> packed::Bytes {
        packed::WitnessArgs::new_builder()
            .input_type(packed::Bytes::from(value.to_le_bytes().to_vec()))
            .build()
            .as_bytes()
            .pack()
    }

    fn empty_cellbase(block_number: u64) -> ckb_types::core::TransactionView {
        TransactionBuilder::default()
            .version(0u32)
            .input(CellInput::new_cellbase_input(block_number))
            .witness(simple_lock_script().into_witness())
            .build()
    }

    fn committed_tx_response(
        tx: &ckb_types::core::TransactionView,
        block_hash: H256,
    ) -> TransactionWithStatusResponse {
        let tx_json: TransactionView = tx.clone().into();
        TransactionWithStatusResponse {
            transaction: Some(ResponseFormat::json(tx_json)),
            cycles: None,
            time_added_to_pool: None,
            tx_status: TxStatus::committed(1u64.into(), block_hash, 0u32.into()),
            fee: None,
            min_replace_fee: None,
        }
    }

    fn lock_script_with_byte(
        byte: u8,
        hash_type: ckb_types::core::ScriptHashType,
    ) -> ckb_types::packed::Script {
        ckb_types::packed::Script::new_builder()
            .code_hash(Byte32::new([byte; 32]))
            .hash_type(hash_type)
            .args(ckb_types::bytes::Bytes::new())
            .build()
    }

    fn lock_script_with_raw_hash_type(byte: u8, hash_type: u8) -> ckb_types::packed::Script {
        ckb_types::packed::Script::new_builder()
            .code_hash(Byte32::new([byte; 32]))
            .hash_type(hash_type)
            .args(ckb_types::bytes::Bytes::new())
            .build()
    }

    fn serve_http_once(
        response_status: &str,
        response_body: String,
    ) -> (
        String,
        std::sync::Arc<Mutex<String>>,
        thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let body = std::sync::Arc::new(Mutex::new(String::new()));
        let captured = body.clone();
        let response_status = response_status.to_string();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 4096];
            loop {
                let read = stream.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let header_end = request
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|idx| idx + 4)
                .unwrap();
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let lower = line.to_ascii_lowercase();
                    lower
                        .strip_prefix("content-length: ")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            while request.len() < header_end + content_length {
                let read = stream.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
            }
            *captured.lock().unwrap() =
                String::from_utf8_lossy(&request[header_end..header_end + content_length]).into();

            let response = format!(
                "HTTP/1.1 {response_status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        (format!("http://127.0.0.1:{}", addr.port()), body, handle)
    }

    #[derive(Clone)]
    struct TestHttpResponse {
        status: String,
        body: String,
        extra_headers: Vec<(String, String)>,
    }

    fn serve_http_sequence(
        responses: Vec<TestHttpResponse>,
    ) -> (String, Arc<AtomicUsize>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let request_count = Arc::new(AtomicUsize::new(0));
        let captured = request_count.clone();
        let handle = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().unwrap();
                captured.fetch_add(1, Ordering::SeqCst);
                let mut request = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let read = stream.read(&mut buffer).unwrap();
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }

                let mut headers = String::new();
                for (name, value) in response.extra_headers {
                    headers.push_str(&format!("{name}: {value}\r\n"));
                }
                let wire_response = format!(
                    "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n{}",
                    response.status,
                    response.body.len(),
                    headers,
                    response.body
                );
                stream.write_all(wire_response.as_bytes()).unwrap();
            }
        });
        (
            format!("http://127.0.0.1:{}", addr.port()),
            request_count,
            handle,
        )
    }

    fn read_log_lines(path: &Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn test_first_startup_audits_tip_and_persists_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let cursor_path = dir.path().join("cursor.json");
        let cfg = test_config(cursor_path.clone());

        let tip_header = HeaderBuilder::default()
            .number(0u64)
            .timestamp(1000u64)
            .epoch(EpochNumberWithFraction::new(0, 0, 1000).full_value())
            .build();
        let tip_block = BlockBuilder::default()
            .header(tip_header.clone())
            .transaction(empty_cellbase(0))
            .build();
        let tip_json: HeaderView = tip_header.into();
        let tip_block_json: BlockView = tip_block.clone().into();

        let rpc = Arc::new(MockRpc::default());
        *rpc.tip.lock().unwrap() = Some(tip_json);
        *rpc.consensus.lock().unwrap() = Some(mock_consensus());
        rpc.blocks_by_number
            .lock()
            .unwrap()
            .insert(0, tip_block_json);

        let auditor = Auditor::new(rpc, cfg);
        let mut cursor = None;
        auditor.poll_once(&mut cursor).await.unwrap();

        let loaded = CursorState::load(&cursor_path).await.unwrap().unwrap();
        assert_eq!(loaded.last_height, 0);
        assert_eq!(
            loaded.genesis_hash,
            Some(format!("{:#x}", H256::from([1u8; 32])))
        );
    }

    #[tokio::test]
    async fn test_no_cursor_mode_audits_tip_tracks_intermediate_blocks_and_does_not_create_files() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("audit.log");
        let mut cfg = test_config(dir.path().join("cursor.json"));
        cfg.cursor_path = None;
        cfg.log_path = Some(log_path.clone());

        let block_0: BlockView = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(0u64)
                    .timestamp(1000u64)
                    .epoch(EpochNumberWithFraction::new(0, 0, 1000).full_value())
                    .build(),
            )
            .transaction(empty_cellbase(0))
            .build()
            .into();
        let block_1: BlockView = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(1u64)
                    .timestamp(1010u64)
                    .epoch(EpochNumberWithFraction::new(0, 1, 1000).full_value())
                    .parent_hash(block_0.header.hash.pack())
                    .build(),
            )
            .transaction(empty_cellbase(1))
            .build()
            .into();
        let block_2: BlockView = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(2u64)
                    .timestamp(1020u64)
                    .epoch(EpochNumberWithFraction::new(0, 2, 1000).full_value())
                    .parent_hash(block_1.header.hash.pack())
                    .build(),
            )
            .transaction(empty_cellbase(2))
            .build()
            .into();
        let block_4: BlockView = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(4u64)
                    .timestamp(1040u64)
                    .epoch(EpochNumberWithFraction::new(0, 4, 1000).full_value())
                    .build(),
            )
            .transaction(empty_cellbase(4))
            .build()
            .into();
        let block_5: BlockView = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(5u64)
                    .timestamp(1050u64)
                    .epoch(EpochNumberWithFraction::new(0, 5, 1000).full_value())
                    .parent_hash(block_4.header.hash.pack())
                    .build(),
            )
            .transaction(empty_cellbase(5))
            .build()
            .into();

        let rpc = Arc::new(MockRpc::default());
        *rpc.consensus.lock().unwrap() = Some(mock_consensus());
        *rpc.tip.lock().unwrap() = Some(block_0.header.clone());
        rpc.blocks_by_number
            .lock()
            .unwrap()
            .insert(0, block_0.clone());
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(0, block_0.header.clone());
        rpc.headers_by_hash.lock().unwrap().insert(
            format!("{:#x}", block_0.header.hash),
            block_0.header.clone(),
        );

        let auditor = Auditor::new(rpc.clone(), cfg.clone());
        let mut cursor = None;
        auditor.poll_once(&mut cursor).await.unwrap();
        assert_eq!(read_log_lines(&log_path).len(), 1);
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            1,
            "only the explicit log file should be created"
        );

        *rpc.tip.lock().unwrap() = Some(block_2.header.clone());
        rpc.blocks_by_number
            .lock()
            .unwrap()
            .insert(1, block_1.clone());
        rpc.blocks_by_number
            .lock()
            .unwrap()
            .insert(2, block_2.clone());
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(1, block_1.header.clone());
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(2, block_2.header.clone());
        rpc.headers_by_hash.lock().unwrap().insert(
            format!("{:#x}", block_1.header.hash),
            block_1.header.clone(),
        );
        rpc.headers_by_hash.lock().unwrap().insert(
            format!("{:#x}", block_2.header.hash),
            block_2.header.clone(),
        );

        auditor.poll_once(&mut cursor).await.unwrap();
        auditor.poll_once(&mut cursor).await.unwrap();
        let lines = read_log_lines(&log_path);
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0]["block_height"], 0);
        assert_eq!(lines[1]["block_height"], 1);
        assert_eq!(lines[2]["block_height"], 2);

        *rpc.tip.lock().unwrap() = Some(block_5.header.clone());
        rpc.blocks_by_number
            .lock()
            .unwrap()
            .insert(5, block_5.clone());
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(4, block_4.header.clone());
        rpc.headers_by_hash.lock().unwrap().insert(
            format!("{:#x}", block_4.header.hash),
            block_4.header.clone(),
        );
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(5, block_5.header.clone());
        let restarted = Auditor::new(rpc, cfg);
        let mut restart_cursor = None;
        restarted.poll_once(&mut restart_cursor).await.unwrap();
        let lines = read_log_lines(&log_path);
        assert_eq!(lines.len(), 4);
        assert_eq!(lines[3]["block_height"], 5);
    }

    #[tokio::test]
    async fn test_missing_consensus_does_not_create_or_advance_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let cursor_path = dir.path().join("cursor.json");
        let cfg = test_config(cursor_path.clone());
        let tip_header = HeaderBuilder::default()
            .number(7u64)
            .epoch(EpochNumberWithFraction::new(0, 7, 1000).full_value())
            .build();
        let tip_block: BlockView = BlockBuilder::default()
            .header(tip_header.clone())
            .build()
            .into();

        let rpc = Arc::new(MockRpc::default());
        *rpc.tip.lock().unwrap() = Some(tip_header.into());
        rpc.blocks_by_number.lock().unwrap().insert(7, tip_block);

        let auditor = Auditor::new(rpc, cfg);
        let mut cursor = None;
        let err = auditor
            .poll_once(&mut cursor)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("consensus prerequisite failed"));
        assert!(cursor.is_none());
        assert!(!cursor_path.exists());
    }

    #[tokio::test]
    async fn test_cursor_genesis_mismatch_is_rejected_without_overwriting_file() {
        let dir = tempfile::tempdir().unwrap();
        let cursor_path = dir.path().join("cursor.json");
        let existing = CursorState {
            genesis_hash: Some(format!("{:#x}", H256::from([1u8; 32]))),
            last_height: 9,
            last_hash: format!("{:#x}", H256::from([9u8; 32])),
            next_height: None,
            next_hash: None,
            history: BTreeMap::from([(9, format!("{:#x}", H256::from([9u8; 32])))]),
        };
        existing.save(&cursor_path).await.unwrap();
        let original = std::fs::read_to_string(&cursor_path).unwrap();

        let cfg = test_config(cursor_path.clone());
        let tip_header = HeaderBuilder::default()
            .number(10u64)
            .epoch(EpochNumberWithFraction::new(0, 10, 1000).full_value())
            .build();
        let rpc = Arc::new(MockRpc::default());
        *rpc.tip.lock().unwrap() = Some(tip_header.into());
        *rpc.consensus.lock().unwrap() = Some(mock_consensus_with_id_and_limit(
            "ckb",
            H256::from([3u8; 32]),
            0x1000,
        ));

        let auditor = Auditor::new(rpc, cfg);
        let mut cursor = auditor.load_cursor().await.unwrap();
        let err = auditor
            .poll_once(&mut cursor)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("cursor genesis hash"));
        assert_eq!(std::fs::read_to_string(&cursor_path).unwrap(), original);
    }

    #[tokio::test]
    async fn test_legacy_cursor_migrates_only_after_matching_history_verification() {
        let dir = tempfile::tempdir().unwrap();
        let cursor_path = dir.path().join("cursor.json");
        let header_100 = HeaderBuilder::default()
            .number(100u64)
            .epoch(EpochNumberWithFraction::new(0, 100, 1000).full_value())
            .build();
        let header_101 = HeaderBuilder::default()
            .number(101u64)
            .epoch(EpochNumberWithFraction::new(0, 101, 1000).full_value())
            .parent_hash(header_100.hash())
            .build();
        let block_101: BlockView = BlockBuilder::default()
            .header(header_101.clone())
            .build()
            .into();
        std::fs::write(
            &cursor_path,
            serde_json::to_vec_pretty(&json!({
                "last_height": 100,
                "last_hash": format!("{:#x}", header_100.hash()),
                "history": {
                    "100": format!("{:#x}", header_100.hash())
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let cfg = test_config(cursor_path.clone());

        let rpc = Arc::new(MockRpc::default());
        *rpc.consensus.lock().unwrap() = Some(mock_consensus_with_id_and_limit(
            "compatible-devnet",
            H256::from([1u8; 32]),
            0x1000,
        ));
        *rpc.tip.lock().unwrap() = Some(header_101.clone().into());
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(100, header_100.clone().into());
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(101, header_101.clone().into());
        rpc.headers_by_hash.lock().unwrap().insert(
            format!("{:#x}", header_100.hash()),
            header_100.clone().into(),
        );
        rpc.blocks_by_number.lock().unwrap().insert(101, block_101);

        let auditor = Auditor::new(rpc, cfg);
        let mut cursor = auditor.load_cursor().await.unwrap();
        auditor.poll_once(&mut cursor).await.unwrap();

        let migrated: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&cursor_path).unwrap()).unwrap();
        assert_eq!(
            migrated["genesis_hash"],
            serde_json::Value::String(format!("{:#x}", H256::from([1u8; 32])))
        );
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
        *rpc.consensus.lock().unwrap() = Some(mock_consensus());
        rpc.headers_by_hash.lock().unwrap().insert(
            format!("{:#x}", block_json.header.inner.parent_hash),
            parent_json.clone(),
        );
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(parent_json.inner.number.value(), parent_json.clone());
        rpc.headers_by_number.lock().unwrap().insert(
            block_json.header.inner.number.value(),
            block_json.header.clone(),
        );
        rpc.blocks_by_hash
            .lock()
            .unwrap()
            .insert(format!("{:#x}", block.hash()), block_json.clone());
        rpc.txs.lock().unwrap().insert(
            format!("{:#x}", prev_tx.hash()),
            serde_json::to_value(tx_resp).unwrap(),
        );
        rpc.economics
            .lock()
            .unwrap()
            .insert(format!("{:#x}", parent.hash()), economic);

        let auditor = Auditor::new(rpc, cfg);
        let log = auditor.audit_block(&block_json).await;
        assert_eq!(log.check_ordinary_capacity_conservation, CheckStatus::Fail);
        assert_eq!(log.result, AuditResult::Fail);
        assert!(
            log.failed_checks
                .as_ref()
                .is_some_and(|checks| !checks.is_empty())
        );
    }

    #[tokio::test]
    async fn test_http_get_transaction_uses_full_committed_wire_params() {
        let prev_tx = TransactionBuilder::default()
            .version(0u32)
            .output(simple_cell_output(100))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();
        let prev_tx_json: TransactionView = prev_tx.clone().into();
        let tx_resp = TransactionWithStatusResponse {
            transaction: Some(ResponseFormat::json(prev_tx_json)),
            cycles: None,
            time_added_to_pool: None,
            tx_status: TxStatus::committed(1u64.into(), H256::from([9u8; 32]), 0u32.into()),
            fee: None,
            min_replace_fee: None,
        };
        let response_body = serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": tx_resp,
        }))
        .unwrap();
        let (url, body, handle) = serve_http_once("200 OK", response_body);
        let rpc = HttpRpc::new(url, 5, 0).unwrap();
        let hash = prev_tx.hash().unpack();

        let response = rpc.get_transaction(&hash).await.unwrap().unwrap();
        handle.join().unwrap();
        let captured: serde_json::Value = serde_json::from_str(&body.lock().unwrap()).unwrap();
        assert_eq!(captured["method"], "get_transaction");
        assert_eq!(
            captured["params"],
            json!([format!("{hash:#x}"), "0x2", true])
        );
        assert_eq!(response.tx_status.status, Status::Committed);
    }

    #[tokio::test]
    async fn test_http_get_block_by_number_rejects_invalid_lock_hash_type_from_wire() {
        let block = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(1u64)
                    .epoch(EpochNumberWithFraction::new(0, 1, 1000).full_value())
                    .build(),
            )
            .transaction(
                TransactionBuilder::default()
                    .version(0u32)
                    .input(CellInput::new_cellbase_input(1))
                    .output(simple_cell_output(100))
                    .output_data(ckb_types::bytes::Bytes::new())
                    .witness(simple_lock_script().into_witness())
                    .build(),
            )
            .build();
        let mut block_value = serde_json::to_value(BlockView::from(block)).unwrap();
        block_value["transactions"][0]["outputs"][0]["lock"]["hash_type"] = json!("data255");
        let response_body = serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": block_value,
        }))
        .unwrap();
        let (url, _body, handle) = serve_http_once("200 OK", response_body);
        let rpc = HttpRpc::new(url, 5, 0).unwrap();

        let err = rpc.get_block_by_number(1).await.unwrap_err().to_string();
        handle.join().unwrap();

        assert!(err.contains("rpc get_block_by_number result decode failed"));
    }

    #[tokio::test]
    async fn test_http_rpc_error_does_not_leak_credentials() {
        let (url, _body, handle) = serve_http_once(
            "500 Internal Server Error",
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"boom"}}"#.to_string(),
        );
        let auth_url = format!(
            "http://{}:{}@{}",
            "user",
            "secret",
            url.trim_start_matches("http://")
        );
        let rpc = HttpRpc::new(auth_url, 5, 0).unwrap();
        let err = rpc.get_consensus().await.unwrap_err().to_string();
        handle.join().unwrap();

        assert!(err.contains("http status 500"));
        assert!(!err.contains("secret"));
        assert!(!err.contains("user:secret"));
    }

    #[test]
    fn test_parse_retry_after_priority_helpers() {
        let now = DateTime::parse_from_rfc3339("2026-09-14T08:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            parse_retry_after_value("60", now).unwrap(),
            std::time::Duration::from_secs(60)
        );
        assert_eq!(
            parse_retry_after_value("Mon, 14 Sep 2026 08:31:00 GMT", now).unwrap(),
            std::time::Duration::from_secs(60)
        );
        assert_eq!(
            parse_retry_after_from_body(
                r#"{"error":{"message":"allowed qps exceeded: Too many requests (exceeds 2000), try again after 60s"}}"#
            )
            .unwrap(),
            std::time::Duration::from_secs(60)
        );
        assert!(parse_retry_after_value("not-a-delay", now).is_none());
        assert!(parse_retry_after_value("Mon, 14 Sep 2026 08:29:59 GMT", now).is_none());
    }

    #[test]
    fn test_retry_after_zero_header_uses_body_delay() {
        let clock = Arc::new(ManualRpcClock::new(
            DateTime::parse_from_rfc3339("2026-09-14T08:30:00Z")
                .unwrap()
                .with_timezone(&Utc),
        ));
        let rpc = HttpRpc::new_for_test(
            "http://127.0.0.1:8114".to_string(),
            5,
            0,
            CancellationToken::new(),
            clock,
        )
        .unwrap();
        let decision = rpc.parse_retry_after_delay(
            Some(&reqwest::header::HeaderValue::from_static("0")),
            r#"{"error":{"message":"allowed qps exceeded: Too many requests (exceeds 2000), try again after 60s"}}"#,
        );
        assert_eq!(decision.source, "response_body");
        assert_eq!(decision.delay, std::time::Duration::from_secs(60));
    }

    #[test]
    fn test_retry_after_fallback_is_used_when_header_and_body_are_unusable() {
        let clock = Arc::new(ManualRpcClock::new(
            DateTime::parse_from_rfc3339("2026-09-14T08:30:00Z")
                .unwrap()
                .with_timezone(&Utc),
        ));
        let rpc = HttpRpc::new_for_test(
            "http://127.0.0.1:8114".to_string(),
            5,
            0,
            CancellationToken::new(),
            clock,
        )
        .unwrap();
        let decision = rpc.parse_retry_after_delay(
            Some(&reqwest::header::HeaderValue::from_static("bogus")),
            r#"{"error":{"message":"rate limited"}}"#,
        );
        assert_eq!(decision.source, "fallback");
        assert_eq!(
            decision.delay,
            std::time::Duration::from_secs(RATE_LIMIT_FALLBACK_DELAY_SECS)
        );
    }

    #[test]
    fn test_pending_audit_window_rate_limits_repeated_diagnostics() {
        let mut window = PendingAuditWindow::new(4);
        let first = window.observe_pending(
            std::time::Duration::ZERO,
            "0xabc",
            "same".to_string(),
            Some(std::time::Duration::from_secs(15)),
        );
        assert!(first.should_log);
        assert_eq!(first.rounds, 1);
        for round in 2..=10 {
            let observation = window.observe_pending(
                std::time::Duration::from_secs(round as u64),
                "0xabc",
                "same".to_string(),
                Some(std::time::Duration::from_secs(15)),
            );
            assert!(
                !observation.should_log,
                "round {round} should be suppressed"
            );
        }
        let reminder = window.observe_pending(
            std::time::Duration::from_secs(11),
            "0xabc",
            "same".to_string(),
            Some(std::time::Duration::from_secs(15)),
        );
        assert!(reminder.should_log);
        assert_eq!(reminder.repeats_since_change, 10);
        assert_eq!(
            window.next_retry_not_before("0xabc", std::time::Duration::ZERO),
            Some(std::time::Duration::from_secs(15))
        );
    }

    #[test]
    fn test_pending_audit_window_logs_reason_changes_and_recovers_once() {
        let mut window = PendingAuditWindow::new(2);
        let _ = window.observe_pending(
            std::time::Duration::ZERO,
            "0xabc",
            "first".to_string(),
            None,
        );
        let changed = window.observe_pending(
            std::time::Duration::from_secs(1),
            "0xabc",
            "second".to_string(),
            None,
        );
        assert!(changed.should_log);
        assert!(changed.reason_changed);
        let recovered = window.resolve("0xabc").unwrap();
        assert_eq!(recovered.rounds, 2);
        assert!(window.resolve("0xabc").is_none());
    }

    #[tokio::test]
    async fn test_non_retryable_pending_block_uses_in_memory_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().join("cursor.json"));
        cfg.cursor_path = None;
        cfg.max_retries = 0;
        let consensus = ConsensusSnapshot::from_rpc(&cfg, mock_consensus()).unwrap();

        let parent_block = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(1u64)
                    .timestamp(1000u64)
                    .epoch(EpochNumberWithFraction::new(0, 1, 1000).full_value())
                    .build(),
            )
            .transaction(empty_cellbase(1))
            .build();
        let deposit_tx = TransactionBuilder::default()
            .version(0u32)
            .output(dao_cell_output(&consensus, 10_000_000_000))
            .output_data(le_u64_bytes(0))
            .build();
        let withdrawing_tx = TransactionBuilder::default()
            .version(0u32)
            .input(CellInput::new(PackedOutPoint::new(deposit_tx.hash(), 0), 0))
            .output(dao_cell_output(&consensus, 10_000_000_000))
            .output_data(le_u64_bytes(1))
            .build();
        let final_tx = TransactionBuilder::default()
            .version(0u32)
            .header_dep(parent_block.header().hash())
            .input(CellInput::new(
                PackedOutPoint::new(withdrawing_tx.hash(), 0),
                0,
            ))
            .witness(witness_with_input_type_u64(0))
            .output(simple_cell_output(13_000_000_000))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();
        let block_2 = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(2u64)
                    .timestamp(1200u64)
                    .epoch(EpochNumberWithFraction::new(0, 2, 1000).full_value())
                    .parent_hash(parent_block.header().hash())
                    .build(),
            )
            .transaction(empty_cellbase(2))
            .transaction(final_tx)
            .build();
        let block_2_json: BlockView = block_2.into();
        let parent_json: HeaderView = parent_block.header().to_owned().into();

        let rpc = Arc::new(MockRpc::default());
        *rpc.consensus.lock().unwrap() = Some(mock_consensus());
        *rpc.tip.lock().unwrap() = Some(block_2_json.header.clone());
        rpc.headers_by_hash
            .lock()
            .unwrap()
            .insert(format!("{:#x}", parent_json.hash), parent_json.clone());
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(1, parent_json.clone());
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(2, block_2_json.header.clone());
        rpc.blocks_by_number
            .lock()
            .unwrap()
            .insert(2, block_2_json.clone());
        rpc.txs.lock().unwrap().insert(
            format!("{:#x}", withdrawing_tx.hash()),
            serde_json::to_value(committed_tx_response(
                &withdrawing_tx,
                parent_block.header().hash().unpack(),
            ))
            .unwrap(),
        );
        rpc.dao_errors.lock().unwrap().insert(
            format!("{:#x}:{}", deposit_tx.hash(), 0),
            "invalid params: malformed dao reference".to_string(),
        );

        let clock = Arc::new(ManualRpcClock::new(
            DateTime::parse_from_rfc3339("2026-09-14T08:30:00Z")
                .unwrap()
                .with_timezone(&Utc),
        ));
        let auditor = Auditor::new_with_dependencies(
            rpc.clone(),
            cfg,
            CancellationToken::new(),
            clock.clone(),
        );
        let mut cursor = None;
        auditor.poll_once(&mut cursor).await.unwrap();
        assert_eq!(rpc.dao_calls.lock().unwrap().len(), 1);

        auditor.poll_once(&mut cursor).await.unwrap();
        assert_eq!(rpc.dao_calls.lock().unwrap().len(), 1);

        clock.advance(std::time::Duration::from_secs(
            PENDING_EXECUTION_BACKOFF_FLOOR_SECS,
        ));
        auditor.poll_once(&mut cursor).await.unwrap();
        assert_eq!(rpc.dao_calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_http_429_cooldown_is_shared_across_clones_and_methods() {
        let header = HeaderBuilder::default()
            .number(9u64)
            .epoch(EpochNumberWithFraction::new(0, 9, 1000).full_value())
            .build();
        let response_body = serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": HeaderView::from(header.clone()),
        }))
        .unwrap();
        let (url, request_count, handle) = serve_http_sequence(vec![
            TestHttpResponse {
                status: "429 Too Many Requests".to_string(),
                body: r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"allowed qps exceeded: Too many requests (exceeds 2000), try again after 60s"}}"#.to_string(),
                extra_headers: vec![("Retry-After".to_string(), "60".to_string())],
            },
            TestHttpResponse {
                status: "200 OK".to_string(),
                body: response_body,
                extra_headers: vec![],
            },
        ]);
        let shutdown = CancellationToken::new();
        let clock = Arc::new(ManualRpcClock::new(
            DateTime::parse_from_rfc3339("2026-09-14T08:30:00Z")
                .unwrap()
                .with_timezone(&Utc),
        ));
        let rpc = HttpRpc::new_for_test(url, 5, 0, shutdown, clock.clone()).unwrap();
        let clone = rpc.clone();

        let err = rpc.get_consensus().await.unwrap_err().to_string();
        assert!(err.contains("http status 429"));
        assert_eq!(request_count.load(Ordering::SeqCst), 1);

        let wait = tokio::spawn(async move { clone.get_tip_header().await });
        tokio::task::yield_now().await;
        assert_eq!(request_count.load(Ordering::SeqCst), 1);

        clock.advance(std::time::Duration::from_secs(59));
        tokio::task::yield_now().await;
        assert_eq!(request_count.load(Ordering::SeqCst), 1);

        clock.advance(std::time::Duration::from_secs(1));
        let header = wait.await.unwrap().unwrap().unwrap();
        assert_eq!(request_count.load(Ordering::SeqCst), 2);
        assert_eq!(header.inner.number.value(), 9);
        handle.join().unwrap();
    }

    #[test]
    fn test_http_429_extension_persists_across_calls() {
        let clock = Arc::new(ManualRpcClock::new(
            DateTime::parse_from_rfc3339("2026-09-14T08:30:00Z")
                .unwrap()
                .with_timezone(&Utc),
        ));
        let rpc = HttpRpc::new_for_test(
            "http://127.0.0.1:8114".to_string(),
            5,
            1,
            CancellationToken::new(),
            clock.clone(),
        )
        .unwrap();
        rpc.record_cooldown(
            "get_consensus",
            RetryDelayDecision {
                delay: std::time::Duration::from_secs(60),
                source: "retry_after_header",
            },
            "first",
        );
        clock.advance(std::time::Duration::from_secs(60));
        rpc.record_cooldown(
            "get_tip_header",
            RetryDelayDecision {
                delay: std::time::Duration::from_secs(120),
                source: "response_body",
            },
            "second",
        );
        let remaining = rpc
            .current_cooldown_deadline()
            .unwrap()
            .saturating_sub(clock.now());
        assert_eq!(remaining, std::time::Duration::from_secs(120));
    }

    #[tokio::test]
    async fn test_http_429_cooldown_wait_can_be_cancelled() {
        let (url, request_count, handle) = serve_http_sequence(vec![TestHttpResponse {
            status: "429 Too Many Requests".to_string(),
            body: r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"allowed qps exceeded: Too many requests (exceeds 2000), try again after 60s"}}"#.to_string(),
            extra_headers: vec![("Retry-After".to_string(), "60".to_string())],
        }]);
        let shutdown = CancellationToken::new();
        let clock = Arc::new(ManualRpcClock::new(
            DateTime::parse_from_rfc3339("2026-09-14T08:30:00Z")
                .unwrap()
                .with_timezone(&Utc),
        ));
        let rpc = HttpRpc::new_for_test(url, 5, 0, shutdown.clone(), clock.clone()).unwrap();
        let err = rpc.get_consensus().await.unwrap_err().to_string();
        assert!(err.contains("http status 429"));
        assert_eq!(request_count.load(Ordering::SeqCst), 1);

        let blocked = {
            let rpc = rpc.clone();
            tokio::spawn(async move { rpc.get_tip_header().await })
        };
        tokio::task::yield_now().await;
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        shutdown.cancel();

        let cancelled = blocked.await.unwrap().unwrap_err();
        assert!(is_shutdown_error(&cancelled));
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
        handle.join().unwrap();
    }

    #[tokio::test]
    async fn test_http_rate_gate_enforces_min_spacing_across_concurrent_calls() {
        let header = HeaderBuilder::default()
            .number(9u64)
            .epoch(EpochNumberWithFraction::new(0, 9, 1000).full_value())
            .build();
        let response_body = serde_json::to_string(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": HeaderView::from(header.clone()),
        }))
        .unwrap();
        let (url, request_count, handle) = serve_http_sequence(vec![
            TestHttpResponse {
                status: "200 OK".to_string(),
                body: response_body.clone(),
                extra_headers: vec![],
            },
            TestHttpResponse {
                status: "200 OK".to_string(),
                body: response_body,
                extra_headers: vec![],
            },
        ]);
        let clock = Arc::new(ManualRpcClock::new(
            DateTime::parse_from_rfc3339("2026-09-15T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        ));
        let rpc = HttpRpc::new_with_dependencies(
            url,
            5,
            0,
            HttpRpcPacingConfig {
                min_interval_ms: 500,
                max_interval_ms: 2000,
                max_concurrency: 8,
            },
            CancellationToken::new(),
            clock.clone(),
        )
        .unwrap();
        let clone = rpc.clone();

        let first = tokio::spawn(async move { clone.get_tip_header().await });
        tokio::task::yield_now().await;
        assert_eq!(request_count.load(Ordering::SeqCst), 1);

        let second_rpc = rpc.clone();
        let second = tokio::spawn(async move { second_rpc.get_tip_header().await });
        tokio::task::yield_now().await;
        assert_eq!(
            request_count.load(Ordering::SeqCst),
            1,
            "second call should be paced before send"
        );

        clock.advance(std::time::Duration::from_millis(499));
        tokio::task::yield_now().await;
        assert_eq!(request_count.load(Ordering::SeqCst), 1);

        clock.advance(std::time::Duration::from_millis(1));
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(request_count.load(Ordering::SeqCst), 2);
        let metrics = rpc.metrics_snapshot().unwrap();
        assert_eq!(metrics.total_http_attempts, 2);
        assert!(metrics.rate_gate_wait_ms >= 500);
        handle.join().unwrap();
    }

    #[tokio::test]
    async fn test_resolve_dao_deposit_input_uses_original_capacity_without_rpc() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path().join("cursor.json"));
        let consensus = ConsensusSnapshot::from_rpc(&cfg, mock_consensus()).unwrap();

        let deposit_header = HeaderBuilder::default()
            .number(42u64)
            .epoch(EpochNumberWithFraction::new(0, 42, 1000).full_value())
            .build();
        let deposit_tx = TransactionBuilder::default()
            .version(0u32)
            .output(dao_cell_output(&consensus, 10_000_000_000))
            .output_data(le_u64_bytes(0))
            .build();
        let current_tx = TransactionBuilder::default()
            .version(0u32)
            .input(CellInput::new(PackedOutPoint::new(deposit_tx.hash(), 0), 0))
            .output(dao_cell_output(&consensus, 10_000_000_000))
            .output_data(le_u64_bytes(42))
            .build();

        let rpc = Arc::new(MockRpc::default());
        let auditor = Auditor::new(rpc.clone(), cfg);
        let current_tx_json: TransactionView = current_tx.into();
        let source = ResolvedCommittedTransaction {
            tx: deposit_tx.into(),
            block_hash: deposit_header.hash().unpack(),
            block_number: Some(42),
        };

        let capacity = auditor
            .resolve_dao_input_capacity(&consensus, &current_tx_json, 0, &source, 0)
            .await
            .unwrap();

        assert_eq!(capacity, 10_000_000_000);
        assert!(rpc.dao_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_resolve_final_dao_withdraw_uses_witness_header_and_source_block() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path().join("cursor.json"));
        let consensus = ConsensusSnapshot::from_rpc(&cfg, mock_consensus()).unwrap();

        let deposit_header = HeaderBuilder::default()
            .number(7u64)
            .epoch(EpochNumberWithFraction::new(0, 7, 1000).full_value())
            .build();
        let unrelated_header = HeaderBuilder::default()
            .number(70u64)
            .epoch(EpochNumberWithFraction::new(0, 70, 1000).full_value())
            .build();
        let wrong_last_header = HeaderBuilder::default()
            .number(99u64)
            .epoch(EpochNumberWithFraction::new(0, 99, 1000).full_value())
            .build();
        let deposit_tx = TransactionBuilder::default()
            .version(0u32)
            .output(dao_cell_output(&consensus, 100))
            .output_data(le_u64_bytes(0))
            .build();
        let ordinary_prev = TransactionBuilder::default()
            .version(0u32)
            .output(simple_cell_output(1))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();
        let withdrawing_tx = TransactionBuilder::default()
            .version(0u32)
            .input(CellInput::new(
                PackedOutPoint::new(ordinary_prev.hash(), 0),
                0,
            ))
            .input(CellInput::new(PackedOutPoint::new(deposit_tx.hash(), 0), 0))
            .output(simple_cell_output(1))
            .output_data(ckb_types::bytes::Bytes::new())
            .output(dao_cell_output(&consensus, 100))
            .output_data(le_u64_bytes(7))
            .build();
        let current_tx = TransactionBuilder::default()
            .version(0u32)
            .header_dep(unrelated_header.hash())
            .header_dep(deposit_header.hash())
            .header_dep(wrong_last_header.hash())
            .input(CellInput::new(
                PackedOutPoint::new(withdrawing_tx.hash(), 1),
                0,
            ))
            .witness(witness_with_input_type_u64(1))
            .output(simple_cell_output(130))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();

        let rpc = Arc::new(MockRpc::default());
        rpc.headers_by_hash.lock().unwrap().insert(
            format!("{:#x}", deposit_header.hash()),
            deposit_header.clone().into(),
        );
        rpc.dao_max
            .lock()
            .unwrap()
            .insert(format!("{:#x}:{}", deposit_tx.hash(), 0), 130u64.into());
        let auditor = Auditor::new(rpc.clone(), cfg);
        let current_tx_json: TransactionView = current_tx.into();
        let source = ResolvedCommittedTransaction {
            tx: withdrawing_tx.into(),
            block_hash: H256::from([5u8; 32]),
            block_number: Some(8),
        };

        let capacity = auditor
            .resolve_dao_input_capacity(&consensus, &current_tx_json, 0, &source, 1)
            .await
            .unwrap();

        assert_eq!(capacity, 130);
        let calls = rpc.dao_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, format!("{:#x}:{}", deposit_tx.hash(), 0));
        assert_eq!(calls[0].1, format!("header:{:#x}", H256::from([5u8; 32])));
    }

    #[tokio::test]
    async fn test_resolve_dao_input_rejects_invalid_data_length() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path().join("cursor.json"));
        let consensus = ConsensusSnapshot::from_rpc(&cfg, mock_consensus()).unwrap();
        let deposit_tx = TransactionBuilder::default()
            .version(0u32)
            .output(dao_cell_output(&consensus, 100))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();
        let current_tx = TransactionBuilder::default()
            .version(0u32)
            .input(CellInput::new(PackedOutPoint::new(deposit_tx.hash(), 0), 0))
            .output(dao_cell_output(&consensus, 100))
            .output_data(le_u64_bytes(1))
            .build();

        let auditor = Auditor::new(Arc::new(MockRpc::default()), cfg);
        let err = auditor
            .resolve_dao_input_capacity(
                &consensus,
                &TransactionView::from(current_tx),
                0,
                &ResolvedCommittedTransaction {
                    tx: deposit_tx.into(),
                    block_hash: H256::from([4u8; 32]),
                    block_number: Some(1),
                },
                0,
            )
            .await
            .unwrap_err();

        assert_eq!(err.status, CheckStatus::Fail);
        assert_eq!(err.error_code, "DAO_INPUT_DATA_INVALID");
    }

    #[tokio::test]
    async fn test_no_cursor_mode_retries_pending_dao_height_then_advances() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("audit.log");
        let mut cfg = test_config(dir.path().join("cursor.json"));
        cfg.cursor_path = None;
        cfg.log_path = Some(log_path.clone());
        cfg.max_retries = 1;
        let consensus = ConsensusSnapshot::from_rpc(&cfg, mock_consensus()).unwrap();

        let parent_block = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(1u64)
                    .timestamp(1000u64)
                    .epoch(EpochNumberWithFraction::new(0, 1, 1000).full_value())
                    .build(),
            )
            .transaction(empty_cellbase(1))
            .build();
        let deposit_tx = TransactionBuilder::default()
            .version(0u32)
            .output(dao_cell_output(&consensus, 100))
            .output_data(le_u64_bytes(0))
            .build();
        let ordinary_prev = TransactionBuilder::default()
            .version(0u32)
            .output(simple_cell_output(1))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();
        let withdrawing_tx = TransactionBuilder::default()
            .version(0u32)
            .input(CellInput::new(
                PackedOutPoint::new(ordinary_prev.hash(), 0),
                0,
            ))
            .input(CellInput::new(PackedOutPoint::new(deposit_tx.hash(), 0), 0))
            .output(simple_cell_output(10_000_000_000))
            .output_data(ckb_types::bytes::Bytes::new())
            .output(dao_cell_output(&consensus, 100))
            .output_data(le_u64_bytes(1))
            .build();
        let final_tx = TransactionBuilder::default()
            .version(0u32)
            .header_dep(
                HeaderBuilder::default()
                    .number(9u64)
                    .epoch(EpochNumberWithFraction::new(0, 9, 1000).full_value())
                    .build()
                    .hash(),
            )
            .header_dep(parent_block.header().hash())
            .input(CellInput::new(
                PackedOutPoint::new(withdrawing_tx.hash(), 1),
                0,
            ))
            .witness(witness_with_input_type_u64(1))
            .output(simple_cell_output(13_000_000_000))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();
        let block_2 = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(2u64)
                    .timestamp(1200u64)
                    .epoch(EpochNumberWithFraction::new(0, 2, 1000).full_value())
                    .parent_hash(parent_block.header().hash())
                    .build(),
            )
            .transaction(empty_cellbase(2))
            .transaction(final_tx.clone())
            .build();
        let block_3 = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(3u64)
                    .timestamp(1300u64)
                    .epoch(EpochNumberWithFraction::new(0, 3, 1000).full_value())
                    .parent_hash(block_2.header().hash())
                    .build(),
            )
            .transaction(empty_cellbase(3))
            .build();
        let block_2_json: BlockView = block_2.clone().into();
        let block_3_json: BlockView = block_3.into();
        let parent_json: HeaderView = parent_block.header().to_owned().into();

        let rpc = Arc::new(MockRpc::default());
        *rpc.consensus.lock().unwrap() = Some(mock_consensus());
        *rpc.tip.lock().unwrap() = Some(block_2_json.header.clone());
        rpc.headers_by_hash
            .lock()
            .unwrap()
            .insert(format!("{:#x}", parent_json.hash), parent_json.clone());
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(1, parent_json.clone());
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(2, block_2_json.header.clone());
        rpc.headers_by_hash.lock().unwrap().insert(
            format!("{:#x}", block_2_json.header.hash),
            block_2_json.header.clone(),
        );
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(3, block_3_json.header.clone());
        rpc.blocks_by_number
            .lock()
            .unwrap()
            .insert(2, block_2_json.clone());
        rpc.blocks_by_number
            .lock()
            .unwrap()
            .insert(3, block_3_json.clone());
        rpc.txs.lock().unwrap().insert(
            format!("{:#x}", withdrawing_tx.hash()),
            serde_json::to_value(committed_tx_response(
                &withdrawing_tx,
                parent_block.header().hash().unpack(),
            ))
            .unwrap(),
        );

        let auditor = Auditor::new(rpc.clone(), cfg);
        let mut cursor = None;
        auditor.poll_once(&mut cursor).await.unwrap();
        assert!(!log_path.exists());
        assert_eq!(cursor.as_ref().unwrap().next_height, Some(2));

        rpc.dao_max.lock().unwrap().insert(
            format!("{:#x}:{}", deposit_tx.hash(), 0),
            13_000_000_000u64.into(),
        );
        *rpc.tip.lock().unwrap() = Some(block_3_json.header.clone());
        auditor.poll_once(&mut cursor).await.unwrap();

        let lines = read_log_lines(&log_path);
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["block_height"], 2);
        assert_eq!(lines[1]["block_height"], 3);
        assert_eq!(lines[0]["result"], "PASS");
        assert_eq!(lines[1]["result"], "PASS");
        assert!(!dir.path().join("cursor.json").exists());
    }

    #[tokio::test]
    async fn test_reward_uses_finalized_target_block_lock() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path().join("cursor.json"));
        let target_lock = lock_script_with_byte(7, ckb_types::core::ScriptHashType::Data);
        let current_witness_lock = lock_script_with_byte(8, ckb_types::core::ScriptHashType::Data);

        let target_header = HeaderBuilder::default()
            .number(1u64)
            .timestamp(1000u64)
            .epoch(EpochNumberWithFraction::new(0, 0, 1000).full_value())
            .build();

        let target_cellbase = TransactionBuilder::default()
            .version(0u32)
            .input(CellInput::new_cellbase_input(1))
            .output(
                CellOutput::new_builder()
                    .capacity(10_000_000_000u64)
                    .lock(target_lock.clone())
                    .type_(ScriptOpt::default())
                    .build(),
            )
            .output_data(ckb_types::bytes::Bytes::new())
            .witness(target_lock.clone().into_witness())
            .build();
        let current_cellbase = TransactionBuilder::default()
            .version(0u32)
            .input(CellInput::new_cellbase_input(2))
            .output(
                CellOutput::new_builder()
                    .capacity(10_000_000_000u64)
                    .lock(target_lock.clone())
                    .type_(ScriptOpt::default())
                    .build(),
            )
            .output_data(ckb_types::bytes::Bytes::new())
            .witness(current_witness_lock.into_witness())
            .build();

        let target_block = BlockBuilder::default()
            .header(target_header.clone())
            .transaction(target_cellbase)
            .build();
        let canonical_target_header = target_block.header().to_owned();
        let current_header = HeaderBuilder::default()
            .number(2u64)
            .timestamp(1200u64)
            .epoch(EpochNumberWithFraction::new(0, 1, 1000).full_value())
            .parent_hash(canonical_target_header.hash())
            .build();
        let current_block = BlockBuilder::default()
            .header(current_header.clone())
            .transaction(current_cellbase)
            .build();
        let target_block_json: BlockView = target_block.clone().into();
        let current_block_json: BlockView = current_block.clone().into();

        let economic = BlockEconomicState {
            issuance: Default::default(),
            miner_reward: ckb_jsonrpc_types::MinerReward {
                primary: Capacity::from(10_000_000_000u64),
                secondary: Capacity::from(0u64),
                committed: Capacity::from(0u64),
                proposal: Capacity::from(0u64),
            },
            txs_fee: Capacity::from(0u64),
            finalized_at: current_block.hash().unpack(),
        };

        let rpc = Arc::new(MockRpc::default());
        let mut consensus_value = serde_json::to_value(mock_consensus_with_limit(0x1000)).unwrap();
        consensus_value["tx_proposal_window"] = json!({"closest": "0x0", "farthest": "0x0"});
        *rpc.consensus.lock().unwrap() = Some(serde_json::from_value(consensus_value).unwrap());
        rpc.headers_by_hash.lock().unwrap().insert(
            format!("{:#x}", canonical_target_header.hash()),
            canonical_target_header.clone().into(),
        );
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(1, canonical_target_header.clone().into());
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(2, current_header.clone().into());
        rpc.blocks_by_hash.lock().unwrap().insert(
            format!("{:#x}", canonical_target_header.hash()),
            target_block_json,
        );
        rpc.economics
            .lock()
            .unwrap()
            .insert(format!("{:#x}", canonical_target_header.hash()), economic);

        let auditor = Auditor::new(rpc, cfg);
        let log = auditor.audit_block(&current_block_json).await;
        eprintln!("{:?}", serde_json::to_value(&log).unwrap());
        assert_eq!(log.check_cellbase_reward_amount, CheckStatus::Pass);
        assert_eq!(log.check_cellbase_reward_target, CheckStatus::Pass);
        assert!(log.details.is_none());
    }

    #[tokio::test]
    async fn test_unresolved_input_keeps_unknowns_visible() {
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
            .output(simple_cell_output(100))
            .output_data(ckb_types::bytes::Bytes::new())
            .witness(simple_lock_script().into_witness())
            .build();
        let missing_prev = PackedOutPoint::new(Byte32::new([5u8; 32]), 0);
        let spend_tx = TransactionBuilder::default()
            .version(0u32)
            .input(CellInput::new(missing_prev, 0))
            .output(simple_cell_output(1))
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
            .transaction(cellbase)
            .transaction(spend_tx)
            .build();
        let block_json: BlockView = block.clone().into();

        let rpc = Arc::new(MockRpc::default());
        *rpc.consensus.lock().unwrap() = Some(mock_consensus());
        rpc.headers_by_hash
            .lock()
            .unwrap()
            .insert(format!("{:#x}", parent.hash()), parent.clone().into());
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(2, block_json.header.clone());

        let auditor = Auditor::new(rpc, cfg);
        let log = auditor.audit_block(&block_json).await;
        let serialized = serde_json::to_value(&log).unwrap();
        assert_eq!(log.check_input_content_resolution, CheckStatus::Fail);
        assert_eq!(log.check_input_output_index, CheckStatus::Fail);
        assert_eq!(log.check_ordinary_capacity_conservation, CheckStatus::Fail);
        assert_eq!(log.check_dao_withdraw_capacity, CheckStatus::Fail);
        assert!(serialized.get("check_dao_withdraw_capacity").is_some());
        assert!(serialized.get("coverage").is_none());
        assert!(serialized.get("unknown_checks").is_none());
        assert!(
            log.failed_checks.as_ref().is_some_and(
                |checks| checks.contains(&"check_input_content_resolution".to_string())
            )
        );
        assert!(log.details.as_ref().unwrap().iter().any(|detail| {
            detail.referenced_out_point.as_deref().is_some()
                && detail.failure_kind == Some(FailureKind::RetryExhausted)
        }));
    }

    #[tokio::test]
    async fn test_required_null_retry_recovers_to_pass() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().join("cursor.json"));
        cfg.max_retries = 1;

        let parent_block = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(1u64)
                    .timestamp(1000u64)
                    .epoch(EpochNumberWithFraction::new(0, 1, 1000).full_value())
                    .build(),
            )
            .transaction(empty_cellbase(1))
            .build();
        let prev_tx = TransactionBuilder::default()
            .version(0u32)
            .output(simple_cell_output(10_000_000_000))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();
        let spend_tx = TransactionBuilder::default()
            .version(0u32)
            .input(CellInput::new(PackedOutPoint::new(prev_tx.hash(), 0), 0))
            .output(simple_cell_output(10_000_000_000))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();
        let block = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(2u64)
                    .timestamp(1200u64)
                    .epoch(EpochNumberWithFraction::new(0, 2, 1000).full_value())
                    .parent_hash(parent_block.header().hash())
                    .build(),
            )
            .transaction(empty_cellbase(2))
            .transaction(spend_tx.clone())
            .build();
        let block_json: BlockView = block.clone().into();
        let parent_json: HeaderView = parent_block.header().to_owned().into();

        let rpc = Arc::new(RetryRpc::default());
        *rpc.base.consensus.lock().unwrap() = Some(mock_consensus());
        rpc.base
            .headers_by_hash
            .lock()
            .unwrap()
            .insert(format!("{:#x}", parent_json.hash), parent_json.clone());
        rpc.base
            .headers_by_number
            .lock()
            .unwrap()
            .insert(1, parent_json);
        rpc.base
            .headers_by_number
            .lock()
            .unwrap()
            .insert(2, block_json.header.clone());
        rpc.base
            .blocks_by_number
            .lock()
            .unwrap()
            .insert(2, block_json.clone());
        rpc.queue_tx_sequence(
            &prev_tx.hash().unpack(),
            vec![
                None,
                Some(
                    serde_json::to_value(committed_tx_response(
                        &prev_tx,
                        parent_block.header().hash().unpack(),
                    ))
                    .unwrap(),
                ),
            ],
        );

        let auditor = Auditor::new(rpc.clone(), cfg.clone());
        let consensus = ConsensusSnapshot::from_rpc(&cfg, mock_consensus()).unwrap();
        let outcome = auditor
            .audit_height_with_retries(2, &consensus)
            .await
            .unwrap()
            .unwrap();
        let HeightAuditOutcome::Finalized { log, .. } = outcome else {
            panic!("expected finalized audit");
        };

        assert_eq!(log.result, AuditResult::Pass);
        assert!(log.failed_checks.is_none());
        assert!(log.details.is_none());
        assert_eq!(rpc.tx_calls(&prev_tx.hash().unpack()), 2);
        let value = serde_json::to_value(&log).unwrap();
        assert!(value.get("coverage").is_none());
        assert!(value.get("unknown_checks").is_none());
    }

    #[tokio::test]
    async fn test_no_cursor_mode_keeps_retrying_failed_tip_when_tip_advances() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("audit.log");
        let mut cfg = test_config(dir.path().join("cursor.json"));
        cfg.cursor_path = None;
        cfg.log_path = Some(log_path.clone());
        cfg.max_retries = 1;

        let parent_block = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(1u64)
                    .timestamp(1000u64)
                    .epoch(EpochNumberWithFraction::new(0, 1, 1000).full_value())
                    .build(),
            )
            .transaction(empty_cellbase(1))
            .build();
        let prev_tx = TransactionBuilder::default()
            .version(0u32)
            .output(simple_cell_output(10_000_000_000))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();
        let spend_tx = TransactionBuilder::default()
            .version(0u32)
            .input(CellInput::new(PackedOutPoint::new(prev_tx.hash(), 0), 0))
            .output(simple_cell_output(10_000_000_000))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();
        let block_2 = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(2u64)
                    .timestamp(1200u64)
                    .epoch(EpochNumberWithFraction::new(0, 2, 1000).full_value())
                    .parent_hash(parent_block.header().hash())
                    .build(),
            )
            .transaction(empty_cellbase(2))
            .transaction(spend_tx.clone())
            .build();
        let block_3 = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(3u64)
                    .timestamp(1300u64)
                    .epoch(EpochNumberWithFraction::new(0, 3, 1000).full_value())
                    .parent_hash(block_2.header().hash())
                    .build(),
            )
            .transaction(empty_cellbase(3))
            .build();
        let block_2_json: BlockView = block_2.clone().into();
        let block_3_json: BlockView = block_3.into();
        let parent_json: HeaderView = parent_block.header().to_owned().into();

        let rpc = Arc::new(RetryRpc::default());
        *rpc.base.consensus.lock().unwrap() = Some(mock_consensus());
        *rpc.base.tip.lock().unwrap() = Some(block_2_json.header.clone());
        rpc.base
            .headers_by_hash
            .lock()
            .unwrap()
            .insert(format!("{:#x}", parent_json.hash), parent_json.clone());
        rpc.base
            .headers_by_number
            .lock()
            .unwrap()
            .insert(1, parent_json);
        rpc.base
            .headers_by_number
            .lock()
            .unwrap()
            .insert(2, block_2_json.header.clone());
        rpc.base
            .headers_by_number
            .lock()
            .unwrap()
            .insert(3, block_3_json.header.clone());
        rpc.base
            .blocks_by_number
            .lock()
            .unwrap()
            .insert(2, block_2_json.clone());
        rpc.base
            .blocks_by_number
            .lock()
            .unwrap()
            .insert(3, block_3_json.clone());
        rpc.queue_tx_sequence(&prev_tx.hash().unpack(), vec![None, None, None, None]);

        let auditor = Auditor::new(rpc.clone(), cfg);
        let mut cursor = None;
        auditor.poll_once(&mut cursor).await.unwrap();
        let pending = cursor.as_ref().unwrap();
        assert_eq!(pending.last_height, 1);
        assert_eq!(pending.next_height, Some(2));

        *rpc.base.tip.lock().unwrap() = Some(block_3_json.header.clone());
        auditor.poll_once(&mut cursor).await.unwrap();

        assert!(!log_path.exists());
        let pending = cursor.as_ref().unwrap();
        assert_eq!(pending.last_height, 1);
        assert_eq!(pending.next_height, Some(2));
        assert!(!dir.path().join("cursor.json").exists());
    }

    #[tokio::test]
    async fn test_persistent_cursor_retries_pending_height_after_restart_then_advances() {
        let dir = tempfile::tempdir().unwrap();
        let cursor_path = dir.path().join("cursor.json");
        let log_path = dir.path().join("audit.log");
        let mut cfg = test_config(cursor_path.clone());
        cfg.log_path = Some(log_path.clone());
        cfg.max_retries = 1;

        let parent_block = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(1u64)
                    .timestamp(1000u64)
                    .epoch(EpochNumberWithFraction::new(0, 1, 1000).full_value())
                    .build(),
            )
            .transaction(empty_cellbase(1))
            .build();
        let prev_tx = TransactionBuilder::default()
            .version(0u32)
            .output(simple_cell_output(10_000_000_000))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();
        let spend_tx = TransactionBuilder::default()
            .version(0u32)
            .input(CellInput::new(PackedOutPoint::new(prev_tx.hash(), 0), 0))
            .output(simple_cell_output(10_000_000_000))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();
        let block_2 = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(2u64)
                    .timestamp(1200u64)
                    .epoch(EpochNumberWithFraction::new(0, 2, 1000).full_value())
                    .parent_hash(parent_block.header().hash())
                    .build(),
            )
            .transaction(empty_cellbase(2))
            .transaction(spend_tx.clone())
            .build();
        let block_3 = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(3u64)
                    .timestamp(1300u64)
                    .epoch(EpochNumberWithFraction::new(0, 3, 1000).full_value())
                    .parent_hash(block_2.header().hash())
                    .build(),
            )
            .transaction(empty_cellbase(3))
            .build();
        let block_2_json: BlockView = block_2.clone().into();
        let block_3_json: BlockView = block_3.clone().into();
        let parent_json: HeaderView = parent_block.header().to_owned().into();

        let rpc_fail = Arc::new(RetryRpc::default());
        *rpc_fail.base.consensus.lock().unwrap() = Some(mock_consensus());
        *rpc_fail.base.tip.lock().unwrap() = Some(block_2_json.header.clone());
        rpc_fail
            .base
            .headers_by_hash
            .lock()
            .unwrap()
            .insert(format!("{:#x}", parent_json.hash), parent_json.clone());
        rpc_fail
            .base
            .headers_by_number
            .lock()
            .unwrap()
            .insert(1, parent_json.clone());
        rpc_fail
            .base
            .headers_by_number
            .lock()
            .unwrap()
            .insert(2, block_2_json.header.clone());
        rpc_fail.base.headers_by_hash.lock().unwrap().insert(
            format!("{:#x}", block_2_json.header.hash),
            block_2_json.header.clone(),
        );
        rpc_fail
            .base
            .blocks_by_number
            .lock()
            .unwrap()
            .insert(2, block_2_json.clone());
        rpc_fail.queue_tx_sequence(&prev_tx.hash().unpack(), vec![None, None]);

        let auditor_fail = Auditor::new(rpc_fail, cfg.clone());
        let mut cursor = None;
        auditor_fail.poll_once(&mut cursor).await.unwrap();

        let saved = CursorState::load(&cursor_path).await.unwrap().unwrap();
        assert_eq!(saved.last_height, 1);
        assert_eq!(saved.next_height, Some(2));

        let rpc_recover = Arc::new(RetryRpc::default());
        *rpc_recover.base.consensus.lock().unwrap() = Some(mock_consensus());
        *rpc_recover.base.tip.lock().unwrap() = Some(block_3_json.header.clone());
        rpc_recover
            .base
            .headers_by_hash
            .lock()
            .unwrap()
            .insert(format!("{:#x}", parent_json.hash), parent_json);
        rpc_recover
            .base
            .headers_by_number
            .lock()
            .unwrap()
            .insert(1, parent_block.header().to_owned().into());
        rpc_recover
            .base
            .headers_by_number
            .lock()
            .unwrap()
            .insert(2, block_2_json.header.clone());
        rpc_recover.base.headers_by_hash.lock().unwrap().insert(
            format!("{:#x}", block_2_json.header.hash),
            block_2_json.header.clone(),
        );
        rpc_recover
            .base
            .headers_by_number
            .lock()
            .unwrap()
            .insert(3, block_3_json.header.clone());
        rpc_recover
            .base
            .blocks_by_number
            .lock()
            .unwrap()
            .insert(2, block_2_json.clone());
        rpc_recover
            .base
            .blocks_by_number
            .lock()
            .unwrap()
            .insert(3, block_3_json.clone());
        rpc_recover.base.txs.lock().unwrap().insert(
            format!("{:#x}", prev_tx.hash()),
            serde_json::to_value(committed_tx_response(
                &prev_tx,
                parent_block.header().hash().unpack(),
            ))
            .unwrap(),
        );

        let auditor_recover = Auditor::new(rpc_recover, cfg);
        let mut restarted_cursor = auditor_recover.load_cursor().await.unwrap();
        auditor_recover
            .poll_once(&mut restarted_cursor)
            .await
            .unwrap();

        let lines = read_log_lines(&log_path);
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["block_height"], 2);
        assert_eq!(lines[1]["block_height"], 3);
        assert_eq!(lines[0]["result"], "PASS");
        assert_eq!(lines[1]["result"], "PASS");

        let final_cursor = CursorState::load(&cursor_path).await.unwrap().unwrap();
        assert_eq!(final_cursor.last_height, 3);
        assert!(final_cursor.next_height.is_none());
    }

    #[tokio::test]
    async fn test_mixed_rule_fail_and_retry_only_outputs_final_fail_once() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("audit.log");
        let mut cfg = test_config(dir.path().join("cursor.json"));
        cfg.cursor_path = None;
        cfg.log_path = Some(log_path.clone());

        let parent_block = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(1u64)
                    .timestamp(1000u64)
                    .epoch(EpochNumberWithFraction::new(0, 1, 1000).full_value())
                    .build(),
            )
            .transaction(empty_cellbase(1))
            .build();
        let prev_tx = TransactionBuilder::default()
            .version(0u32)
            .output(simple_cell_output(10_000_000_000))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();
        let spend_tx = TransactionBuilder::default()
            .version(0u32)
            .input(CellInput::new(PackedOutPoint::new(prev_tx.hash(), 0), 0))
            .output(simple_cell_output(10_000_000_000))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();
        let block = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(2u64)
                    .version(1u32)
                    .timestamp(1200u64)
                    .epoch(EpochNumberWithFraction::new(0, 2, 1000).full_value())
                    .parent_hash(parent_block.header().hash())
                    .build(),
            )
            .transaction(empty_cellbase(2))
            .transaction(spend_tx.clone())
            .build();
        let block_json: BlockView = block.clone().into();
        let parent_json: HeaderView = parent_block.header().to_owned().into();

        let rpc = Arc::new(RetryRpc::default());
        *rpc.base.consensus.lock().unwrap() = Some(mock_consensus());
        *rpc.base.tip.lock().unwrap() = Some(block_json.header.clone());
        rpc.base
            .headers_by_hash
            .lock()
            .unwrap()
            .insert(format!("{:#x}", parent_json.hash), parent_json.clone());
        rpc.base
            .headers_by_number
            .lock()
            .unwrap()
            .insert(1, parent_json);
        rpc.base
            .headers_by_number
            .lock()
            .unwrap()
            .insert(2, block_json.header.clone());
        rpc.base
            .blocks_by_number
            .lock()
            .unwrap()
            .insert(2, block_json.clone());
        rpc.queue_tx_sequence(&prev_tx.hash().unpack(), vec![None]);

        let auditor = Auditor::new(rpc.clone(), cfg);
        let mut cursor = None;
        auditor.poll_once(&mut cursor).await.unwrap();
        assert!(!log_path.exists());

        rpc.base.txs.lock().unwrap().insert(
            format!("{:#x}", prev_tx.hash()),
            serde_json::to_value(committed_tx_response(
                &prev_tx,
                parent_block.header().hash().unpack(),
            ))
            .unwrap(),
        );
        auditor.poll_once(&mut cursor).await.unwrap();

        let lines = read_log_lines(&log_path);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["result"], "FAIL");
        assert!(
            lines[0]["failed_checks"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item == "check_block_version")
        );
    }

    #[tokio::test]
    async fn test_cursor_save_failure_does_not_duplicate_finalized_log_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let cursor_dir = dir.path().join("cursor-dir");
        std::fs::create_dir(&cursor_dir).unwrap();
        let log_path = dir.path().join("audit.log");
        let mut cfg = test_config(cursor_dir.clone());
        cfg.log_path = Some(log_path.clone());

        let block = BlockBuilder::default()
            .header(HeaderBuilder::default().number(0u64).build())
            .transaction(empty_cellbase(0))
            .build();
        let block_json: BlockView = block.clone().into();
        let header_json: HeaderView = block.header().to_owned().into();

        let rpc = Arc::new(MockRpc::default());
        *rpc.consensus.lock().unwrap() = Some(mock_consensus());
        *rpc.tip.lock().unwrap() = Some(header_json.clone());
        rpc.headers_by_number.lock().unwrap().insert(0, header_json);
        rpc.blocks_by_number.lock().unwrap().insert(0, block_json);

        let auditor = Auditor::new(rpc.clone(), cfg.clone());
        let mut cursor = None;
        let err = auditor
            .poll_once(&mut cursor)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("failed to replace cursor file"));
        let lines = read_log_lines(&log_path);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["result"], "PASS");

        let mut restart_cfg = cfg;
        restart_cfg.cursor_path = Some(dir.path().join("cursor.json"));
        let restarted = Auditor::new(rpc, restart_cfg);
        let mut restarted_cursor = None;
        restarted.poll_once(&mut restarted_cursor).await.unwrap();

        let lines = read_log_lines(&log_path);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["result"], "PASS");
    }

    #[tokio::test]
    async fn test_same_height_reorg_with_different_hash_outputs_both_final_results() {
        let dir = tempfile::tempdir().unwrap();
        let cursor_path = dir.path().join("cursor.json");
        let log_path = dir.path().join("audit.log");
        let mut cfg = test_config(cursor_path.clone());
        cfg.log_path = Some(log_path.clone());

        let parent_block = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(1u64)
                    .epoch(EpochNumberWithFraction::new(0, 1, 1000).full_value())
                    .build(),
            )
            .transaction(empty_cellbase(1))
            .build();
        let block_a = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(2u64)
                    .timestamp(1200u64)
                    .epoch(EpochNumberWithFraction::new(0, 2, 1000).full_value())
                    .parent_hash(parent_block.header().hash())
                    .build(),
            )
            .transaction(empty_cellbase(2))
            .build();
        let block_b = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(2u64)
                    .timestamp(1300u64)
                    .epoch(EpochNumberWithFraction::new(0, 2, 1000).full_value())
                    .parent_hash(parent_block.header().hash())
                    .build(),
            )
            .transaction(empty_cellbase(2))
            .build();
        let parent_json: HeaderView = parent_block.header().to_owned().into();
        let block_a_json: BlockView = block_a.clone().into();
        let block_b_json: BlockView = block_b.clone().into();

        let rpc = Arc::new(MockRpc::default());
        *rpc.consensus.lock().unwrap() = Some(mock_consensus());
        *rpc.tip.lock().unwrap() = Some(block_a_json.header.clone());
        rpc.headers_by_hash
            .lock()
            .unwrap()
            .insert(format!("{:#x}", parent_json.hash), parent_json.clone());
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(1, parent_json.clone());
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(2, block_a_json.header.clone());
        rpc.blocks_by_number
            .lock()
            .unwrap()
            .insert(2, block_a_json.clone());

        let auditor = Auditor::new(rpc.clone(), cfg);
        let mut cursor = None;
        auditor.poll_once(&mut cursor).await.unwrap();

        *rpc.tip.lock().unwrap() = Some(block_b_json.header.clone());
        rpc.headers_by_number
            .lock()
            .unwrap()
            .insert(2, block_b_json.header.clone());
        rpc.blocks_by_number
            .lock()
            .unwrap()
            .insert(2, block_b_json.clone());
        auditor.poll_once(&mut cursor).await.unwrap();

        let lines = read_log_lines(&log_path);
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["block_height"], 2);
        assert_eq!(lines[1]["block_height"], 2);
        assert_ne!(lines[0]["block_hash"], lines[1]["block_hash"]);
    }

    #[test]
    fn test_failed_checks_summary_survives_zero_max_details() {
        let mut cfg = test_config(PathBuf::from("/tmp/cursor.json"));
        cfg.max_details = 0;
        let block = BlockBuilder::default()
            .header(HeaderBuilder::default().number(0u64).build())
            .build();
        let block_json: BlockView = block.into();
        let auditor = Auditor::new(Arc::new(MockRpc::default()), cfg.clone());
        let mut log = AuditLog::new(&cfg, &block_json);
        log.check_block_hash = CheckStatus::Fail;
        log.check_timestamp = CheckStatus::Unknown;
        auditor.backfill_anomaly_details(&mut log);
        log.finalize(2, 1);

        assert!(log.details_total.unwrap() >= 2);
        assert_eq!(log.details.as_ref().unwrap().len(), 0);
        let failed_checks = log.failed_checks.as_ref().unwrap();
        assert!(failed_checks.contains(&"check_block_hash".to_string()));
        assert!(failed_checks.contains(&"check_timestamp".to_string()));
        assert_eq!(log.check_timestamp, CheckStatus::Fail);
    }

    #[test]
    fn test_serialization_omits_not_applicable_checks() {
        let block = BlockBuilder::default()
            .header(HeaderBuilder::default().number(0u64).build())
            .build();
        let block_json: BlockView = block.into();
        let mut log = AuditLog::new(&test_config(PathBuf::from("/tmp/cursor.json")), &block_json);
        log.check_block_height = CheckStatus::Pass;
        log.check_cellbase_reward_amount = CheckStatus::Unknown;
        log.finalize(1, 0);

        let value = serde_json::to_value(&log).unwrap();
        assert_eq!(value["schema_version"], 4);
        assert!(value.get("check_block_height").is_some());
        assert!(value.get("check_cellbase_reward_amount").is_some());
        assert!(value.get("check_dao_withdraw_capacity").is_none());
        assert!(value.get("network").is_none());
        assert!(value.get("reward_target_block_hash").is_none());
        assert!(value.get("reward_verification_method").is_none());
        assert!(value.get("coverage").is_none());
        assert!(value.get("unknown_checks").is_none());
        assert_eq!(value["result"], "FAIL");
    }

    #[tokio::test]
    async fn test_output_lock_hash_type_accepts_all_consensus_encodings() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path().join("cursor.json"));
        let parent = HeaderBuilder::default()
            .number(1u64)
            .timestamp(1000u64)
            .epoch(EpochNumberWithFraction::new(0, 1, 1000).full_value())
            .build();
        let prev_tx = TransactionBuilder::default()
            .version(0u32)
            .output(simple_cell_output(2_000_000_000_000))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();
        let mut spend_builder = TransactionBuilder::default()
            .version(0u32)
            .input(CellInput::new(PackedOutPoint::new(prev_tx.hash(), 0), 0));
        spend_builder = spend_builder
            .output(
                CellOutput::new_builder()
                    .capacity(10_000_000_000u64)
                    .lock(lock_script_with_raw_hash_type(0, 1))
                    .type_(ScriptOpt::default())
                    .build(),
            )
            .output_data(ckb_types::bytes::Bytes::new());
        for hash_type in 0u8..=254 {
            if !ckb_types::core::ScriptHashType::verify_value(hash_type) || hash_type == 1 {
                continue;
            }
            spend_builder = spend_builder
                .output(
                    CellOutput::new_builder()
                        .capacity(10_000_000_000u64)
                        .lock(lock_script_with_raw_hash_type(hash_type, hash_type))
                        .type_(ScriptOpt::default())
                        .build(),
                )
                .output_data(ckb_types::bytes::Bytes::new());
        }
        let spend_tx = spend_builder.build();
        let block = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(2u64)
                    .timestamp(1200u64)
                    .epoch(EpochNumberWithFraction::new(0, 2, 1000).full_value())
                    .parent_hash(parent.hash())
                    .build(),
            )
            .transaction(empty_cellbase(2))
            .transaction(spend_tx)
            .build();
        let block_json: BlockView = block.clone().into();
        let core_block: CoreBlockView = block.clone();
        let mut log = AuditLog::new(&cfg, &block_json);
        let rpc = Arc::new(MockRpc::default());
        rpc.txs.lock().unwrap().insert(
            format!("{:#x}", prev_tx.hash()),
            serde_json::to_value(committed_tx_response(&prev_tx, parent.hash().unpack())).unwrap(),
        );
        let auditor = Auditor::new(rpc, cfg);
        let consensus = ConsensusSnapshot::from_rpc(&auditor.config, mock_consensus()).unwrap();
        auditor
            .audit_transactions(&block_json, &core_block, &mut log, Some(&consensus))
            .await;
        assert_eq!(log.check_output_lock_hash_type, CheckStatus::Pass);
    }

    #[tokio::test]
    async fn test_header_cache_reuses_ancestor_headers_across_adjacent_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().join("cursor.json"));
        cfg.cursor_path = None;
        cfg.header_cache_capacity = 4096;
        let rpc = Arc::new(MockRpc::default());

        let mut parent_hash = H256::default();
        let mut blocks = Vec::new();
        for height in 0..=60u64 {
            let header = HeaderBuilder::default()
                .number(height)
                .timestamp(1_000 + height * 10)
                .epoch(EpochNumberWithFraction::new(0, height, 1000).full_value())
                .parent_hash(parent_hash.pack())
                .build();
            parent_hash = header.hash().unpack();
            blocks.push(
                BlockBuilder::default()
                    .header(header.clone())
                    .transaction(empty_cellbase(height))
                    .build(),
            );
        }
        for block in &blocks {
            let header_json: HeaderView = block.header().to_owned().into();
            let block_json: BlockView = block.clone().into();
            rpc.headers_by_hash
                .lock()
                .unwrap()
                .insert(format!("{:#x}", header_json.hash), header_json.clone());
            rpc.headers_by_number
                .lock()
                .unwrap()
                .insert(header_json.inner.number.value(), header_json);
            rpc.blocks_by_number
                .lock()
                .unwrap()
                .insert(block_json.header.inner.number.value(), block_json.clone());
            rpc.blocks_by_hash
                .lock()
                .unwrap()
                .insert(format!("{:#x}", block_json.header.hash), block_json);
        }

        let mut consensus_value = serde_json::to_value(mock_consensus()).unwrap();
        consensus_value["median_time_block_count"] = json!("0x25");
        let consensus =
            ConsensusSnapshot::from_rpc(&cfg, serde_json::from_value(consensus_value).unwrap())
                .unwrap();
        let auditor = Auditor::new(rpc.clone(), cfg.clone());

        let first_block_json: BlockView = blocks[50].clone().into();
        let first_core: CoreBlockView = blocks[50].clone();
        let mut first_log = AuditLog::new(&cfg, &first_block_json);
        auditor
            .audit_header_and_block(
                &first_block_json,
                &first_core,
                &mut first_log,
                Some(&consensus),
            )
            .await;
        let header_calls_after_first = rpc.method_calls("get_header");
        assert!(header_calls_after_first > 0);

        let second_block_json: BlockView = blocks[51].clone().into();
        let second_core: CoreBlockView = blocks[51].clone();
        let mut second_log = AuditLog::new(&cfg, &second_block_json);
        auditor
            .audit_header_and_block(
                &second_block_json,
                &second_core,
                &mut second_log,
                Some(&consensus),
            )
            .await;
        let header_calls_after_second = rpc.method_calls("get_header");
        assert_eq!(
            header_calls_after_second.saturating_sub(header_calls_after_first),
            1,
            "warm adjacent block should add at most one header fetch"
        );
    }

    #[tokio::test]
    async fn test_reward_target_block_uses_block_cache_on_reaudit() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().join("cursor.json"));
        cfg.cursor_path = None;
        cfg.block_cache_entries = 8;
        cfg.block_cache_max_bytes = 8 * 1024 * 1024;
        let rpc = Arc::new(MockRpc::default());
        let target_header = HeaderBuilder::default()
            .number(42u64)
            .timestamp(1000u64)
            .epoch(EpochNumberWithFraction::new(0, 42, 1000).full_value())
            .build();
        let target_block = BlockBuilder::default()
            .header(target_header.clone())
            .transaction(empty_cellbase(42))
            .build();
        rpc.blocks_by_hash.lock().unwrap().insert(
            format!("{:#x}", target_block.header().hash()),
            target_block.clone().into(),
        );
        let auditor = Auditor::new(rpc.clone(), cfg);
        let target_hash: H256 = target_block.header().hash().unpack();

        let first = auditor.get_block_cached(&target_hash).await.unwrap();
        assert!(first.is_some());
        let get_block_calls_after_first = rpc.method_calls("get_block");
        assert_eq!(get_block_calls_after_first, 1);

        let second = auditor.get_block_cached(&target_hash).await.unwrap();
        assert!(second.is_some());
        let get_block_calls_after_second = rpc.method_calls("get_block");
        assert_eq!(
            get_block_calls_after_second, get_block_calls_after_first,
            "second reward audit should reuse cached target block"
        );
    }

    #[tokio::test]
    async fn test_block_cache_respects_byte_limit() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().join("cursor.json"));
        cfg.cursor_path = None;
        cfg.block_cache_entries = 8;
        cfg.block_cache_max_bytes = 1;
        let rpc = Arc::new(MockRpc::default());

        let target_header = HeaderBuilder::default()
            .number(42u64)
            .timestamp(1000u64)
            .epoch(EpochNumberWithFraction::new(0, 42, 1000).full_value())
            .build();
        let target_block = BlockBuilder::default()
            .header(target_header.clone())
            .transaction(empty_cellbase(42))
            .build();
        rpc.blocks_by_hash.lock().unwrap().insert(
            format!("{:#x}", target_block.header().hash()),
            target_block.clone().into(),
        );
        let auditor = Auditor::new(rpc.clone(), cfg);
        let target_hash: H256 = target_block.header().hash().unpack();

        let first = auditor.get_block_cached(&target_hash).await.unwrap();
        assert!(first.is_some());
        let calls_after_first = rpc.method_calls("get_block");
        let second = auditor.get_block_cached(&target_hash).await.unwrap();
        assert!(second.is_some());
        let calls_after_second = rpc.method_calls("get_block");
        assert_eq!(
            calls_after_second,
            calls_after_first + 1,
            "tiny byte limit should disable effective block reuse"
        );
    }

    #[tokio::test]
    async fn test_failure_serialization_keeps_all_failed_checks_summary_when_details_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().join("cursor.json"));
        cfg.max_details = 1;

        let proposal_tx = TransactionBuilder::default()
            .output(simple_cell_output(42))
            .output_data(ckb_types::bytes::Bytes::new())
            .build();
        let block = BlockBuilder::default()
            .header(HeaderBuilder::default().number(0u64).build())
            .proposal(proposal_tx.proposal_short_id())
            .build();
        let mut block_json: BlockView = block.clone().into();
        block_json.header.hash = H256::from([9u8; 32]);
        block_json.header.inner.transactions_root = H256::from([8u8; 32]);
        block_json.header.inner.proposals_hash = H256::from([7u8; 32]);
        block_json.header.inner.extra_hash = H256::from([6u8; 32]);
        let core_block: CoreBlockView = block.clone();

        let rpc = Arc::new(MockRpc::default());
        let auditor = Auditor::new(rpc, cfg.clone());
        let mut fail_consensus_value = serde_json::to_value(mock_consensus_with_id_and_limit(
            "ckb",
            H256::from([1u8; 32]),
            1,
        ))
        .unwrap();
        fail_consensus_value["max_block_proposals_limit"] = json!("0x0");
        let fail_consensus = ConsensusSnapshot::from_rpc(
            &cfg,
            serde_json::from_value(fail_consensus_value).unwrap(),
        )
        .unwrap();
        let mut log = AuditLog::new(&cfg, &block_json);
        auditor
            .audit_header_and_block(&block_json, &core_block, &mut log, Some(&fail_consensus))
            .await;
        auditor.backfill_anomaly_details(&mut log);
        log.finalize(1, cfg.max_retries);

        let failed_checks = log.failed_checks.as_ref().unwrap();
        assert!(failed_checks.contains(&"check_block_size".to_string()));
        assert!(failed_checks.contains(&"check_proposal_limit".to_string()));
        assert!(failed_checks.contains(&"check_block_hash".to_string()));
        assert!(failed_checks.contains(&"check_transactions_root".to_string()));
        assert!(failed_checks.contains(&"check_proposals_hash".to_string()));
        assert!(failed_checks.contains(&"check_extra_hash".to_string()));
        assert!(log.details_total.unwrap() >= failed_checks.len());
        assert_eq!(log.details_truncated, Some(true));
        assert_eq!(log.details.as_ref().unwrap().len(), 1);

        let value = serde_json::to_value(&log).unwrap();
        assert!(value.get("network").is_none());
        assert!(value.get("reward_verification_method").is_none());
        assert!(value.get("block_consensus_size_bytes").is_none());
        assert!(value["details_total"].as_u64().unwrap() >= failed_checks.len() as u64);
        assert_eq!(value["details_truncated"], true);
    }

    #[tokio::test]
    async fn test_block_size_uses_consensus_limit_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path().join("cursor.json"));
        let block = BlockBuilder::default()
            .header(HeaderBuilder::default().number(0u64).build())
            .build();
        let block_json: BlockView = block.clone().into();
        let core_block: CoreBlockView = block_json.clone().into();
        let actual_size = core_block.data().serialized_size_without_uncle_proposals() as u64;

        let rpc = Arc::new(MockRpc::default());
        let auditor = Auditor::new(rpc, cfg.clone());
        let pass_consensus =
            ConsensusSnapshot::from_rpc(&cfg, mock_consensus_with_limit(actual_size)).unwrap();

        let mut pass_log = AuditLog::new(&cfg, &block_json);
        auditor
            .audit_header_and_block(
                &block_json,
                &core_block,
                &mut pass_log,
                Some(&pass_consensus),
            )
            .await;
        assert_eq!(pass_log.check_block_size, CheckStatus::Pass);

        let fail_consensus = ConsensusSnapshot::from_rpc(
            &cfg,
            mock_consensus_with_limit(actual_size.saturating_sub(1)),
        )
        .unwrap();
        let mut fail_log = AuditLog::new(&cfg, &block_json);
        auditor
            .audit_header_and_block(
                &block_json,
                &core_block,
                &mut fail_log,
                Some(&fail_consensus),
            )
            .await;
        assert_eq!(fail_log.check_block_size, CheckStatus::Fail);
    }

    #[tokio::test]
    async fn test_block_version_and_uncle_limit_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path().join("cursor.json"));
        let block = BlockBuilder::default()
            .header(HeaderBuilder::default().number(0u64).version(0u32).build())
            .transaction(empty_cellbase(0))
            .build();
        let block_json: BlockView = block.clone().into();
        let core_block: CoreBlockView = block.clone();
        let rpc = Arc::new(MockRpc::default());
        let auditor = Auditor::new(rpc, cfg.clone());

        let mut pass_consensus_value =
            serde_json::to_value(mock_consensus_with_limit(10_000_000)).unwrap();
        pass_consensus_value["max_uncles_num"] = json!("0x0");
        pass_consensus_value["block_version"] = json!("0x0");
        let pass_consensus = ConsensusSnapshot::from_rpc(
            &cfg,
            serde_json::from_value(pass_consensus_value).unwrap(),
        )
        .unwrap();
        let mut pass_log = AuditLog::new(&cfg, &block_json);
        auditor
            .audit_header_and_block(
                &block_json,
                &core_block,
                &mut pass_log,
                Some(&pass_consensus),
            )
            .await;
        assert_eq!(pass_log.check_block_version, CheckStatus::Pass);
        assert_eq!(pass_log.check_uncle_count_limit, CheckStatus::Pass);

        let mut block_version_fail_value =
            serde_json::to_value(mock_consensus_with_limit(10_000_000)).unwrap();
        block_version_fail_value["max_uncles_num"] = json!("0x0");
        block_version_fail_value["block_version"] = json!("0x1");
        let block_version_fail_consensus = ConsensusSnapshot::from_rpc(
            &cfg,
            serde_json::from_value(block_version_fail_value).unwrap(),
        )
        .unwrap();
        let mut block_version_fail_log = AuditLog::new(&cfg, &block_json);
        auditor
            .audit_header_and_block(
                &block_json,
                &core_block,
                &mut block_version_fail_log,
                Some(&block_version_fail_consensus),
            )
            .await;
        assert_eq!(
            block_version_fail_log.check_block_version,
            CheckStatus::Fail
        );
        assert!(
            block_version_fail_log
                .details
                .as_ref()
                .unwrap()
                .iter()
                .any(|detail| detail.error_code == "BLOCK_VERSION_MISMATCH")
        );

        let uncle_source = BlockBuilder::default()
            .header(
                HeaderBuilder::default()
                    .number(7u64)
                    .epoch(EpochNumberWithFraction::new(0, 7, 1000).full_value())
                    .build(),
            )
            .build();
        let block_with_uncle = BlockBuilder::default()
            .header(HeaderBuilder::default().number(0u64).version(0u32).build())
            .uncle(uncle_source.as_uncle())
            .transaction(empty_cellbase(0))
            .build();
        let block_with_uncle_json: BlockView = block_with_uncle.clone().into();
        let block_with_uncle_core: CoreBlockView = block_with_uncle.clone();

        let mut equal_limit_value =
            serde_json::to_value(mock_consensus_with_limit(10_000_000)).unwrap();
        equal_limit_value["max_uncles_num"] = json!("0x1");
        let equal_limit_consensus =
            ConsensusSnapshot::from_rpc(&cfg, serde_json::from_value(equal_limit_value).unwrap())
                .unwrap();
        let mut equal_limit_log = AuditLog::new(&cfg, &block_with_uncle_json);
        auditor
            .audit_header_and_block(
                &block_with_uncle_json,
                &block_with_uncle_core,
                &mut equal_limit_log,
                Some(&equal_limit_consensus),
            )
            .await;
        assert_eq!(equal_limit_log.check_uncle_count_limit, CheckStatus::Pass);

        let mut uncle_fail_value =
            serde_json::to_value(mock_consensus_with_limit(10_000_000)).unwrap();
        uncle_fail_value["max_uncles_num"] = json!("0x0");
        let uncle_fail_consensus =
            ConsensusSnapshot::from_rpc(&cfg, serde_json::from_value(uncle_fail_value).unwrap())
                .unwrap();
        let mut uncle_fail_log = AuditLog::new(&cfg, &block_with_uncle_json);
        auditor
            .audit_header_and_block(
                &block_with_uncle_json,
                &block_with_uncle_core,
                &mut uncle_fail_log,
                Some(&uncle_fail_consensus),
            )
            .await;
        assert_eq!(uncle_fail_log.check_uncle_count_limit, CheckStatus::Fail);
        assert!(
            uncle_fail_log
                .details
                .as_ref()
                .unwrap()
                .iter()
                .any(|detail| detail.error_code == "UNCLE_COUNT_EXCEEDED")
        );
    }
}
