// for running Codes :
// clickhouse-client --user=mehran --password='mehran.crypto9' --multiquery < init_clickhouse.sql

//drop database: DROP DATABASE IF EXISTS pajohesh;


use ethers::prelude::*;
use clickhouse::{Client, Row};
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio;
use anyhow::Result;
use reqwest::Client as HttpClient;

#[derive(Row, Serialize, Deserialize)]
struct WalletRow {
    address: String,
    balance: String,
    nonce: u64,
    last_seen_block: u64,
    #[serde(rename = "type")]
    wallet_type: String,
    defi: String,
    sensitive: u8,
}

#[derive(Row, Serialize, Deserialize)]
struct TransactionRow {
    hash: String,
    block_number: u64,
    tx_index: u32,
    from_addr: String,
    to_addr: String,
    value: String
}

#[tokio::main]
async fn main() -> Result<()> {
    let clickhouse = Arc::new(
        Client::default()
            .with_url("localhost:9000")
            .with_user("mehran")
            .with_password("mehran.crypto9")
            .with_database("pajohesh"),
    );

    let provider = Arc::new(
        Provider::<Http>::try_from(
            "https://rpc.ankr.com/eth/a4ce905377a7aa94ded62bf6efb50b20acde76159d163f8de77a16ec6237137b",
        )?,
    );

    let start_block: u64 = 19000000;
    let mut tx_count = 0;
    let total_txs = 300;

    for block_number in start_block..start_block + 20 {
        if tx_count >= total_txs { break; }

        if let Some(block) = provider.get_block_with_txs(block_number).await? {
            for (tx_index, tx) in block.transactions.iter().enumerate() {
                if tx_count >= total_txs { break; }
                process_tx(
                    &provider,
                    &clickhouse,
                    tx,
                    block_number,
                    tx_index as u32,
                )
                .await?;
                tx_count += 1;
            }
        }
    }

    println!("Done: {} txs fetched", tx_count);
    Ok(())
}

async fn process_tx(
    provider: &Arc<Provider<Http>>,
    clickhouse: &Arc<Client>,
    tx: &Transaction,
    block_number: u64,
    tx_index: u32,
) -> Result<()> {
    let from = Some(tx.from);
    let to = tx.to;

    if let Some(addr) = from {
        save_wallet_clickhouse(
            clickhouse,
            provider,
            addr,
            block_number,
        )
        .await?;
    }

    if let Some(addr) = to {
        save_wallet_clickhouse(
            clickhouse,
            provider,
            addr,
            block_number,
        )
        .await?;
    }

    insert_tx_clickhouse(
        clickhouse,
        tx.hash,
        block_number,
        tx_index,
        from,
        to,
        tx.value.to_string()
    )
    .await?;

    Ok(())
}

async fn save_wallet_clickhouse(
    clickhouse: &Arc<Client>,
    provider: &Arc<Provider<Http>>,
    addr: Address,
    block_number: u64,
) -> Result<()> {
    let balance = provider.get_balance(addr, None).await?;
    let nonce = provider.get_transaction_count(addr, None).await?;
    let wallet_type = detect_wallet_type_from_etherscan(addr).await?;

    let row = WalletRow {
        address: format!("{:#x}", addr),
        balance: balance.to_string(),
        nonce: nonce.as_u64(),
        last_seen_block: block_number,
        wallet_type,
        defi: "".to_string(),
        sensitive: 1,
    };

    let mut insert = clickhouse.insert::<WalletRow>("wallet_info").await?;
    insert.write(&row).await?;
    insert.end().await?;

    Ok(())
}

async fn insert_tx_clickhouse(
    clickhouse: &Arc<Client>,
    tx_hash: H256,
    block_number: u64,
    tx_index: u32,
    from: Option<Address>,
    to: Option<Address>,
    value: String,
) -> Result<()> {
    let row = TransactionRow {
        hash: format!("{:#x}", tx_hash),
        block_number,
        tx_index,
        from_addr: from.map(|a| format!("{:#x}", a)).unwrap_or_default(),
        to_addr: to.map(|a| format!("{:#x}", a)).unwrap_or_default(),
        value
    };

    let mut insert = clickhouse.insert::<TransactionRow>("transactions").await?;
    insert.write(&row).await?;
    insert.end().await?;
    Ok(())
}

#[derive(Deserialize)]
struct EtherscanAbiResult {
    status: String,
    message: String
}

pub async fn detect_wallet_type_from_etherscan(
    address: Address,
) -> anyhow::Result<String> {


    let api_key = "DWYGKM65G8A7HHE4J497BWF9TK3R4H9NGC";
    let url = format!(
        "https://api.etherscan.io/v2/api?chain=eth&chainid=1&module=contract&action=getabi&address={:?}&apikey={}",
        address, api_key
    );

    let client = Arc::new(HttpClient::new());

    let resp = client.get(&url).send().await?;
    let body: EtherscanAbiResult  = resp.json().await?;

    if body.status == "1" && body.message == "OK" {
        return Ok("smart_contract".to_string());
    }

    if body.status == "0" {
        return Ok("wallet".to_string());
    }
    Ok("wallet".to_string())
}
