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
use sqlx::postgres::{PgPool, PgPoolOptions};

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

    // Postgres
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pg_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
            
    // Get Hash topic-Transfer
    let transfer_topic = Transfer::SIGNATURE_HASH.to_string();
    info!("Targeting Transfer topic: {}", transfer_topic);

    loop {
        if let Err(e) = processing_loop(&ch, &pg_pool, &transfer_topic).await {
            error!("Error in decoder loop: {:?}. Sleeping...", e);
            sleep(Duration::from_secs(5)).await;
        }
    }
}

/// Loop decoded
async fn processing_loop(ch: &ClickHouseClient, pg: &PgPool, transfer_topic: &str) -> Result<()> {
    // Find, where we stopped
    let cursor_row = sqlx::query!(
        "SELECT last_processed_block FROM decoder_state WHERE id = 'erc20_worker'"
    )
    .fetch_one(pg)
    .await?;
    
    let start_block = cursor_row.last_processed_block as u64 + 1;

    // Get canonical batch (100) from Postgres
    let canonical_batch = sqlx::query!(
        r#"
        SELECT number, hash 
        FROM canonical_blocks 
        WHERE chain_id = 1 AND status = 'canonical' AND number >= $1 
        ORDER BY number ASC 
        LIMIT 100
        "#,
        start_block as i64
    )
    .fetch_all(pg)
    .await?;

    // Sleep if not new canonical_block
    if canonical_batch.is_empty() {
        info!("Synced. Waiting for canonical blocks...");
        sleep(Duration::from_secs(2)).await;
        return Ok(());
    }

    let end_block = canonical_batch.last().unwrap().number as u64;
    let hashes_str = canonical_batch.iter().map(|b| format!("'{}'", b.hash)).collect::<Vec<_>>().join(",");

    info!("Processing blocks {} -> {} ({} blocks)", start_block, end_block, canonical_batch.len());

    // Query to CH
    let query = format!(
        r#"
        SELECT ?fields 
        FROM raw_logs_head 
        WHERE topic0 = '{}' AND block_hash IN ({})
        ORDER BY block_number ASC
        "#,
        transfer_topic, hashes_str
    );

    let logs: Vec<RawLog> = ch.query(&query).fetch_all().await?;

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
                    from: event.from.to_string().to_lowercase(),
                    to: event.to.to_string().to_lowercase(),
                    value: val_str,
                    value_approx: val_f64
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
        info!("Saved {} transfers", transfers.len());
    }

    sqlx::query!(
        "UPDATE decoder_state SET last_processed_block = $1, updated_at = NOW() WHERE id = 'erc20_worker'",
        end_block as i64
    )
    .execute(pg)
    .await?;

    Ok(())
}