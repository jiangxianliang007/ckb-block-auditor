use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;

use ckb_block_auditor::{Auditor, AuditorConfig, HttpRpc, HttpRpcPacingConfig};

#[derive(Parser, Debug)]
#[command(author, version, about = "CKB Block Auditor V1")]
struct Args {
    #[arg(long, env = "CKB_RPC_URL", default_value = "http://127.0.0.1:8114")]
    rpc_url: String,
    #[arg(long, env = "CKB_NODE_ID", default_value = "unknown")]
    node_id: String,
    #[arg(long, env = "CKB_POLL_INTERVAL_MS", default_value_t = 3000)]
    poll_interval_ms: u64,
    #[arg(long, env = "CKB_RPC_TIMEOUT_SECS", default_value_t = 10)]
    rpc_timeout_secs: u64,
    #[arg(long, env = "CKB_MAX_RETRIES", default_value_t = 2)]
    max_retries: u32,
    #[arg(long, env = "CKB_CURSOR_PATH")]
    cursor_path: Option<std::path::PathBuf>,
    #[arg(long, env = "CKB_LOG_PATH")]
    log_path: Option<PathBuf>,
    #[arg(long, env = "CKB_MAX_DETAILS", default_value_t = 200)]
    max_details: usize,
    #[arg(long, env = "CKB_MAX_FUTURE_MS", default_value_t = 15000)]
    max_future_ms: u64,
    #[arg(long, env = "CKB_MEDIAN_TIME_SPAN", default_value_t = 11)]
    median_time_span: usize,
    #[arg(long, env = "CKB_PROPOSAL_LIMIT", default_value_t = 1500)]
    proposal_limit: usize,
    #[arg(long, env = "CKB_TX_VERSION", default_value_t = 0)]
    tx_version: u32,
    #[arg(long, env = "CKB_HISTORY_RETENTION", default_value_t = 256)]
    history_retention: usize,
    #[arg(long, env = "CKB_DAO_TYPE_HASH", default_value = "")]
    dao_type_hash: String,
    #[arg(long, env = "CKB_RPC_MIN_INTERVAL_MS", default_value_t = 100)]
    rpc_min_interval_ms: u64,
    #[arg(long, env = "CKB_RPC_MAX_INTERVAL_MS", default_value_t = 2000)]
    rpc_max_interval_ms: u64,
    #[arg(long, env = "CKB_RPC_MAX_CONCURRENCY", default_value_t = 2)]
    rpc_max_concurrency: usize,
    #[arg(long, env = "CKB_HEADER_CACHE_CAPACITY", default_value_t = 8192)]
    header_cache_capacity: usize,
    #[arg(long, env = "CKB_BLOCK_CACHE_ENTRIES", default_value_t = 64)]
    block_cache_entries: usize,
    #[arg(long, env = "CKB_BLOCK_CACHE_MAX_BYTES", default_value_t = 64 * 1024 * 1024)]
    block_cache_max_bytes: usize,
    #[arg(long, env = "CKB_STATS_INTERVAL_SECS", default_value_t = 60)]
    stats_interval_secs: u64,
    #[arg(long, env = "CKB_ENABLE_POW_CHECK", default_value_t = true)]
    enable_pow_check: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let cfg = AuditorConfig {
        rpc_url: args.rpc_url.clone(),
        node_id: args.node_id,
        poll_interval_ms: args.poll_interval_ms,
        rpc_timeout_secs: args.rpc_timeout_secs,
        max_retries: args.max_retries,
        cursor_path: args.cursor_path,
        log_path: args.log_path,
        max_details: args.max_details,
        max_future_ms: args.max_future_ms,
        median_time_span: args.median_time_span,
        proposal_limit: args.proposal_limit,
        tx_version: args.tx_version,
        history_retention: args.history_retention,
        dao_type_hash: args.dao_type_hash,
        header_cache_capacity: args.header_cache_capacity,
        block_cache_entries: args.block_cache_entries,
        block_cache_max_bytes: args.block_cache_max_bytes,
        stats_interval_secs: args.stats_interval_secs,
        enable_pow_check: args.enable_pow_check,
    };

    let rpc = Arc::new(HttpRpc::new_with_pacing(
        args.rpc_url,
        cfg.rpc_timeout_secs,
        cfg.max_retries,
        HttpRpcPacingConfig {
            min_interval_ms: args.rpc_min_interval_ms,
            max_interval_ms: args.rpc_max_interval_ms,
            max_concurrency: args.rpc_max_concurrency,
        },
    )?);
    let auditor = Auditor::new(rpc, cfg);
    auditor.run().await
}

#[cfg(test)]
mod tests {
    use super::Args;
    use clap::Parser;

    #[test]
    fn test_cli_cursor_path_is_optional_and_network_is_removed() {
        let args =
            Args::try_parse_from(["ckb-block-auditor", "--rpc-url", "http://localhost:8114"])
                .unwrap();
        assert!(args.cursor_path.is_none());
    }

    #[test]
    fn test_cli_rejects_removed_network_flag() {
        let err = Args::try_parse_from(["ckb-block-auditor", "--network", "mainnet"])
            .unwrap_err()
            .to_string();
        assert!(err.contains("--network"));
    }
}
