use alloy::{
    sol,
    primitives::{Log, LogData},
    sol_types::SolEvent
};
use anyhow::{Result};
use clickhouse::Client as ClickHouseClient;
use common::models::{RawLog, Erc20Transfer};
use dotenv::dotenv;
use std::{
    env,
    time::Duration
};
use tokio::time::sleep;
use tracing::{error, info};

sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);
}

#[tokio::main]
async fn main() -> Result<()> {
    // env
    dotenv().ok();
    // Logs
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
            .add_directive(tracing::Level::INFO.into()))
        .init();

    info!("Starting Decoder Service...");

    // Config ClickHouse
    let ch_url = "http://localhost:8123";
    let ch_user = env::var("CH_USER").unwrap_or("default".to_string());
    let ch_pass = env::var("CH_PASSWORD").unwrap_or("".to_string());

    let ch = ClickHouseClient::default()
        .with_url(ch_url)
        .with_user(&ch_user)
        .with_password(&ch_pass)
        .with_database("onchain_data");

    // Get Hash topic-Transfer
    let transfer_topic = Transfer::SIGNATURE_HASH.to_string();
    info!("Targeting Transfer topic: {}", transfer_topic);

    loop {
        if let Err(e) = processing_loop(&ch, &transfer_topic).await {
            error!("Error in decoder loop: {:?}. Sleeping...", e);
            sleep(Duration::from_secs(5)).await;
        }
    }
}

/// Loop decoded
async fn processing_loop(ch: &ClickHouseClient, transfer_topic: &str) -> Result<()> {
    // Find, where we stopped
    // if db is empty, start with 0 or block with logs
    let start_block = ch
        .query("SELECT toUInt64(coalesce(max(block_number), 0)) FROM erc20_transfers")
        .fetch_one::<u64>()
        .await
        .unwrap_or(0);
    
    // Get batch of logs, whiсh ones are transfers
    // filter on start_block
    let query = format!(
        r#"
        SELECT ?fields
        FROM raw_logs_head
        WHERE topic0 = '{}' AND block_number > {}
        ORDER BY block_number ASC
        LIMIT 5000"#,
        transfer_topic, start_block
    );

    let logs: Vec<RawLog> = ch.query(&query).fetch_all().await?;

    if logs.is_empty() {
        info!("No new logs to decode. Synced at block {}. Sleeping...", start_block);
        sleep(Duration::from_secs(5)).await;
        return Ok(());
    }

    info!("Fetched {} raw logs to decode (starting from block {})", logs.len(), start_block);

    // Decoding
    let mut transfers = Vec::with_capacity(logs.len());

    for log in logs {
        // Parse topics and data
        let t0 = log.topic0.parse().unwrap_or_default();
        let t1 = log.topic1.parse().unwrap_or_default();
        let t2 = log.topic2.parse().unwrap_or_default();
        let data_bytes = hex::decode(log.data.trim_start_matches("0x")).unwrap_or_default();

        let mut topics = vec![t0, t1, t2];
        if !log.topic3.is_empty() && log.topic3 != "0x0000000000000000000000000000000000000000000000000000000000000000" {
            if let Ok(t3) = log.topic3.parse() {
                topics.push(t3);
            }
        }

        let alloy_log = Log {
            address: log.address.parse().unwrap_or_default(),
            data: LogData::new_unchecked(topics, data_bytes.into())
        };

        match Transfer::decode_log(&alloy_log) {
            Ok(event) => {
                let val_str = event.value.to_string();
                let val_f64 = val_str.parse::<f64>().unwrap_or(0.0);

                transfers.push(Erc20Transfer {
                    chain_id: log.chain_id,
                    block_number: log.block_number,
                    block_hash: log.block_hash,
                    tx_hash: log.tx_hash,
                    log_index: log.log_index,
                    block_timestamp: log.block_timestamp,
                    token_address: log.address,
                    from: event.from.to_string(),
                    to: event.to.to_string(),
                    value: val_str,
                    value_numeric: val_f64
                });
            }
            Err(_) => {
                continue;
            }
        }
    }

    if !transfers.is_empty() {
        let mut insert = ch.insert::<Erc20Transfer>("erc20_transfers").await?;
        for row in transfers.iter() {
            insert.write(row).await?;
        }
        insert.end().await?;

        let max_block = transfers.last().map(|t| t.block_number).unwrap_or(start_block);
         info!("Decoded and saved {} transfers (up to block {})", transfers.len(), max_block);
    }

    Ok(())
}