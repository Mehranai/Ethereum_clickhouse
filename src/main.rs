use ethers::prelude::*;
use clickhouse::{Client, Row};
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio;
use anyhow::Result;
use reqwest::Client as HttpClient;
use futures::stream::{FuturesUnordered, StreamExt};

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
    from_addr: String,
    to_addr: String,
    value: String,
}

#[derive(Row, Serialize, Deserialize)]
struct OwnerRow {
    address: String,
    person_name: String,
    person_id: u16,
    personal_id: u16,
}

enum Sensivity {
    LowSensive = 0,
    MiddleSensive = 1,
    HighSensive = 2,
}

#[tokio::main]
async fn main() -> Result<()> {
    let clickhouse = Arc::new(
        Client::default()
            .with_url("http://localhost:8123") // HTTP port
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
    let total_txs = 300;
    let mut tx_count = 0;

    for block_number in start_block..start_block + 20 {
        if tx_count >= total_txs { break; }

        if let Some(block) = provider.get_block_with_txs(block_number).await? {

            let mut tasks = FuturesUnordered::new();
            for tx in block.transactions {
                if tx_count >= total_txs { break; }

                let provider = provider.clone();
                let clickhouse = clickhouse.clone();
                tasks.push(tokio::spawn(async move {
                    process_tx(&provider, &clickhouse, &tx, block_number).await
                }));
                tx_count += 1;
                println!("Tx: #{}", tx_count);
            }
            while let Some(res) = tasks.next().await {
                res??;
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
) -> Result<()> {
    let from = Some(tx.from);
    let to = tx.to;

    if let Some(addr) = from {
        save_wallet_clickhouse(clickhouse, provider, addr, block_number).await?;
    }
    if let Some(addr) = to {
        save_wallet_clickhouse(clickhouse, provider, addr, block_number).await?;
    }

    insert_tx_clickhouse(clickhouse, tx.hash, block_number, from, to, tx.value.to_string()).await?;

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
        sensitive: Sensivity::LowSensive as u8,
    };

    let owner = OwnerRow {
        address: format!("{:#x}", addr),
        person_name: "".to_string(),
        person_id: 0,
        personal_id: 0,
    };

    let mut insert_wallet = clickhouse.insert::<WalletRow>("wallet_info").await?;
    let mut insert_owner = clickhouse.insert::<OwnerRow>("owner_info").await?;

    insert_wallet.write(&row).await?;
    insert_owner.write(&owner).await?;
    insert_wallet.end().await?;
    insert_owner.end().await?;

    Ok(())
}

async fn insert_tx_clickhouse(
    clickhouse: &Arc<Client>,
    tx_hash: H256,
    block_number: u64,
    from: Option<Address>,
    to: Option<Address>,
    value: String,
) -> Result<()> {
    let row = TransactionRow {
        hash: format!("{:#x}", tx_hash),
        block_number,
        from_addr: from.map(|a| format!("{:#x}", a)).unwrap_or_default(),
        to_addr: to.map(|a| format!("{:#x}", a)).unwrap_or_default(),
        value,
    };

    let mut insert = clickhouse.insert::<TransactionRow>("transactions").await?;
    insert.write(&row).await?;
    insert.end().await?;
    Ok(())
}

#[derive(Deserialize)]
struct EtherscanAbiResult {
    status: String,
    message: String,
}

pub async fn detect_wallet_type_from_etherscan(address: Address) -> anyhow::Result<String> {
    let api_key = "DWYGKM65G8A7HHE4J497BWF9TK3R4H9NGC";
    let url = format!(
        "https://api.etherscan.io/v2/api?chain=eth&chainid=1&module=contract&action=getabi&address={:?}&apikey={}",
        address, api_key
    );

    let client = HttpClient::new();
    let resp = client.get(&url).send().await?;
    let body: EtherscanAbiResult = resp.json().await?;

    if body.status == "1" && body.message == "OK" {
        Ok("smart_contract".to_string())
    } else {
        Ok("wallet".to_string())
    }
}
