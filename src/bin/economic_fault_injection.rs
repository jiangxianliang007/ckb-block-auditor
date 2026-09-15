use anyhow::{Context, Result, anyhow, bail};
use ckb_jsonrpc_types::{Block, BlockTemplate, BlockView, TransactionTemplate};
use ckb_types::{H256, bytes::Bytes, core::TransactionBuilder, packed, prelude::*};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

const ONE_CKB: u64 = 100_000_000;

async fn rpc<T: DeserializeOwned>(
    client: &Client,
    url: &str,
    method: &str,
    params: Value,
) -> Result<T> {
    let response = client
        .post(url)
        .json(&json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }))
        .send()
        .await
        .with_context(|| format!("rpc {method} request failed"))?
        .error_for_status()
        .with_context(|| format!("rpc {method} returned http error"))?;

    let value: Value = response
        .json()
        .await
        .with_context(|| format!("rpc {method} returned invalid json"))?;
    if let Some(error) = value.get("error") {
        bail!("rpc {method} failed: {error}");
    }
    let result = value
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow!("rpc {method} response has no result"))?;
    serde_json::from_value(result).with_context(|| format!("decode rpc {method} result"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let rpc_url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:8114".to_string());
    let client = Client::new();

    // Use a committed, non-DAO genesis output as the malicious transaction input.
    // The block itself is later inserted with process_block_without_verify, so no
    // lock witness is required for this fault-injection scenario.
    let genesis: Option<BlockView> = rpc(
        &client,
        &rpc_url,
        "get_block_by_number",
        json!(["0x0", "0x2", false]),
    )
    .await?;
    let genesis = genesis.context("genesis block missing")?;
    let source_tx = genesis
        .transactions
        .first()
        .context("genesis block has no transaction")?;

    let (source_index, source_output_json) = source_tx
        .inner
        .outputs
        .iter()
        .enumerate()
        .find(|(_, output)| output.type_.is_none())
        .context("genesis has no non-DAO output suitable for capacity test")?;

    let source_tx_packed: packed::Transaction = source_tx.inner.clone().into();
    let source_output: packed::CellOutput = source_output_json.clone().into();
    let input_capacity: u64 = source_output.capacity().unpack();
    let output_capacity = input_capacity
        .checked_add(ONE_CKB)
        .context("capacity overflow while constructing malicious transaction")?;

    let malicious_output = source_output.as_builder().capacity(output_capacity).build();
    let out_point = packed::OutPoint::new(source_tx_packed.calc_tx_hash(), source_index as u32);
    let malicious_tx = TransactionBuilder::default()
        .input(packed::CellInput::new(out_point, 0))
        .output(malicious_output)
        .output_data(Bytes::new())
        .build();
    let malicious_hash: H256 = malicious_tx.hash().unpack();

    // Height 1 on the dev chain has a genesis->first-epoch transition that is not
    // relevant to this capacity test. Generate one fully verified block first so
    // the injected block at height 2 isolates the intended economic fault.
    let _: H256 = rpc(&client, &rpc_url, "generate_block", json!([])).await?;

    // Start from a node-generated next-block template so parent/epoch/timestamp,
    // cellbase, extension, proposals and uncle data remain realistic. Replace the
    // transaction list with exactly one deliberately inflationary transaction.
    let mut template: BlockTemplate =
        rpc(&client, &rpc_url, "get_block_template", json!([])).await?;
    template.transactions.clear();
    template.transactions.push(TransactionTemplate {
        hash: malicious_hash.clone(),
        required: true,
        cycles: None,
        depends: None,
        data: malicious_tx.data().into(),
    });

    // BlockTemplate -> packed::Block recomputes transaction/proposal/extra roots,
    // so the injected block is internally hash-consistent. The intended fault is
    // only economic: output capacity is exactly 1 CKB larger than input capacity.
    let packed_block: packed::Block = template.into();
    let expected_block_hash: H256 = packed_block.calc_header_hash().unpack();
    let block: Block = packed_block.into();

    let inserted_hash: Option<H256> = rpc(
        &client,
        &rpc_url,
        "process_block_without_verify",
        json!([block, false]),
    )
    .await?;
    let inserted_hash = inserted_hash.context("node did not accept injected block")?;
    if inserted_hash != expected_block_hash {
        bail!(
            "injected block hash mismatch: expected {expected_block_hash:#x}, got {inserted_hash:#x}"
        );
    }

    let tip: Value = rpc(&client, &rpc_url, "get_tip_header", json!([])).await?;
    let tip_hash = tip
        .get("hash")
        .and_then(Value::as_str)
        .context("tip header missing hash")?;
    if !tip_hash.eq_ignore_ascii_case(&format!("{inserted_hash:#x}")) {
        bail!(
            "injected block did not become canonical tip: inserted={inserted_hash:#x}, tip={tip_hash}"
        );
    }

    println!(
        "injected economic fault: tx={malicious_hash:#x} input_shannons={input_capacity} output_shannons={output_capacity} block={inserted_hash:#x}"
    );
    Ok(())
}
