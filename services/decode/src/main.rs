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
use tracing::{error, info, warn};
use sqlx::postgres::{PgPool, PgPoolOptions};

sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);
}

const SAFE_REORG_DEPTH: i64 = 64;
const BATCH_SIZE: i64 = 500;

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
    let state = sqlx::query!(
        r#"SELECT last_processed_block, last_processed_hash FROM decoder_state WHERE id = 'erc20_worker'"#
    )
    .fetch_one(pg)
    .await?;

    // Find, where we stopped
    let last_processed = state.last_processed_block;
    let last_hash: Option<String> = state.last_processed_hash.map(|h| h.trim().to_string());
    // canonical tip
    let tip = sqlx::query!(
        r#"
        SELECT number, hash
        FROM canonical_blocks
        WHERE chain_id = 1 AND status = 'canonical'
        ORDER BY number DESC
        LIMIT 1
        "#
    )
    .fetch_optional(pg)
    .await?;

    let Some(tip) = tip else {
        info!("No canonical blocks yet. Sleeping...");
        sleep(Duration::from_secs(2)).await;
        return Ok(());
    };

    let tip_number = tip.number;

    if tip_number <= last_processed {
        sleep(Duration::from_secs(2)).await;
        return Ok(());
    }

    // Check if it was reorg 
    let mut reorg_detected = false;
    if last_processed > 0 {
        if let Some(stored_hash) = last_hash.as_deref() {
            let current_hash_row = sqlx::query!(
                r#"
                SELECT hash
                FROM canonical_blocks
                WHERE chain_id = 1 AND status = 'canonical' AND number = $1
                LIMIT 1
                "#,
                last_processed
            )
            .fetch_optional(pg)
            .await?;

            match current_hash_row {
                Some(r) if r.hash != stored_hash => {
                    reorg_detected = true;
                    warn!(
                        "Reorg detected: canonical hash at {} changed (stored={} current={})",
                        last_processed, stored_hash, r.hash
                    );
                }
                None => {
                    reorg_detected = true;
                    warn!(
                        "Reorg/rollback detected: no canonical block at last_processed={}",
                        last_processed
                    );
                }
                _ => {} // Hash matches
            }
        }
    }

    // Calculate Start Block
    let start_scan_block = if reorg_detected {
        (last_processed - SAFE_REORG_DEPTH + 1).max(0)
    } else {
        last_processed + 1
    };

    // Get canonical batch from Postgres
    let canonical_batch = sqlx::query!(
        r#"
        SELECT number, hash
        FROM canonical_blocks
        WHERE chain_id = 1
          AND status = 'canonical'
          AND number >= $1
          AND number <= $2
        ORDER BY number ASC
        LIMIT $3
        "#,
        start_scan_block,
        tip_number,
        BATCH_SIZE
    )
    .fetch_all(pg)
    .await?;

    // Sleep if not new canonical_block
    if canonical_batch.is_empty() {
        info!("Synced. Waiting for canonical blocks...");
        sleep(Duration::from_secs(2)).await;
        return Ok(());
    }

    let end_block = canonical_batch.last().unwrap().number;
    let end_hash = canonical_batch.last().unwrap().hash.clone();
    info!(
        "Scanning blocks {} -> {} ({} blocks)",
        start_scan_block, end_block, canonical_batch.len()
    );

    let hashes_str = canonical_batch.iter().map(|b| format!("'{}'", b.hash)).collect::<Vec<_>>().join(",");

    info!("Scanning blocks {} -> {} (overlap mode)", start_scan_block, end_block);

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
    if !logs.is_empty() {
        let mut transfers = Vec::with_capacity(logs.len());
        let now_base = chrono::Utc::now().timestamp_micros() as u64;

        for (i, log) in logs.iter().enumerate() {
            let t0 = log.topic0.parse().unwrap_or_default();
            let t1 = log.topic1.parse().unwrap_or_default();
            let t2 = log.topic2.parse().unwrap_or_default();
            let data_bytes = hex::decode(log.data.trim_start_matches("0x")).unwrap_or_default();
            
            let mut topics = vec![t0, t1, t2];
            if !log.topic3.is_empty() && log.topic3 != "0x0000000000000000000000000000000000000000000000000000000000000000" {
                if let std::result::Result::Ok(t3) = log.topic3.parse() {
                    topics.push(t3);
                }
            }

            let alloy_log = Log {
                address: log.address.parse().unwrap_or_default(),
                data: LogData::new_unchecked(topics, data_bytes.into()), 
            };

            if let Ok(event) = Transfer::decode_log(&alloy_log) {
                let val_str = event.value.to_string();
                let val_approx = val_str.parse::<f64>().unwrap_or(0.0);    

                let unique_ver = now_base + (i as u64);

                transfers.push(Erc20Transfer {
                    chain_id: log.chain_id,
                    block_number: log.block_number,
                    block_hash: log.block_hash.clone(),
                    tx_hash: log.tx_hash.clone(),
                    log_index: log.log_index,
                    block_timestamp: log.block_timestamp,
                    token_address: log.address.clone(), 
                    from_address: event.from.to_string().to_lowercase(),
                    to_address: event.to.to_string().to_lowercase(),
                    value: val_str,
                    value_approx: val_approx,
                    inserted_at: unique_ver
                });
            }
        }

        if !transfers.is_empty() {
            let mut insert = ch.insert::<Erc20Transfer>("erc20_transfers").await?;
            for row in transfers.iter() { 
                insert.write(row).await?; 
            }
            insert.end().await?;
            info!("Upserted {} transfers", transfers.len());
        }
    }

    sqlx::query!(
        r#"
        UPDATE decoder_state
        SET last_processed_block = $1,
            last_processed_hash = $2,
            updated_at = NOW()
        WHERE id = 'erc20_worker'
        "#,
        end_block,
        end_hash
    )
    .execute(pg)
    .await?;

    info!("Cursor advanced to {} (hash saved)", end_block);

    if end_block >= tip_number {
        sleep(Duration::from_secs(2)).await;
    }

    Ok(())
}