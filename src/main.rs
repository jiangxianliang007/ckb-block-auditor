use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;

use ckb_block_auditor::{Auditor, AuditorConfig, HttpRpc};

#[derive(Parser, Debug)]
#[command(author, version, about = "CKB Block Auditor V1")]
struct Args {
    #[arg(long, env = "CKB_RPC_URL", default_value = "http://127.0.0.1:8114")]
    rpc_url: String,
    #[arg(long, env = "CKB_NETWORK", default_value = "mainnet")]
    network: String,
    #[arg(long, env = "CKB_NODE_ID", default_value = "unknown")]
    node_id: String,
    #[arg(long, env = "CKB_POLL_INTERVAL_MS", default_value_t = 3000)]
    poll_interval_ms: u64,
    #[arg(long, env = "CKB_RPC_TIMEOUT_SECS", default_value_t = 10)]
    rpc_timeout_secs: u64,
    #[arg(long, env = "CKB_MAX_RETRIES", default_value_t = 2)]
    max_retries: u32,
    #[arg(long, env = "CKB_CURSOR_PATH", default_value = "./data/cursor.json")]
    cursor_path: PathBuf,
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
    #[arg(
        long,
        env = "CKB_DAO_TYPE_HASH",
        default_value = "0x82d76d1b75c9f0c49f1437f446f40c6f735af754987eb07f76f4c2f2f5f6f2b2"
    )]
    dao_type_hash: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let cfg = AuditorConfig {
        rpc_url: args.rpc_url.clone(),
        network: args.network,
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
    };

    let rpc = Arc::new(HttpRpc::new(
        args.rpc_url,
        cfg.rpc_timeout_secs,
        cfg.max_retries,
    )?);
    let auditor = Auditor::new(rpc, cfg);
    auditor.run().await
}
