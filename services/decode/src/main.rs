use alloy::{
    sol,
    primitives::{Log, LogData},
    sol_types::SolEvent
};
use anyhow::{Context, Result};
use clickhouse::Client as ClickHouseClient;
use common::{models::{RawLog, Erc20Transfer}, settings::Settings, db::{connect_ch, connect_pg}};
use dotenv::dotenv;
use std::{time::Duration};
use tokio::time::sleep;
use tracing::{error, info, warn};
use sqlx::postgres::{PgPool};
use common::shutdown::shutdown_signal;
use common::metrics::init_metrics;
use metrics::{counter, gauge};

sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);
}

const BATCH_SIZE: i64 = 500;

#[tokio::main]
async fn main() -> Result<()> {
    // env
    dotenv().ok();

    // Connect to metrics
    init_metrics(9092)?; // 9092 port for decode

    // Logs
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
            .add_directive(tracing::Level::INFO.into()))
        .init();

    // Settings
    let settings = Settings::new().context("Config load failed")?;

    // ClickHouse
    let ch = connect_ch(&settings.clickhouse);

    // Postgres
    let pg_pool = connect_pg(&settings.database).await?;
            
    // Get Hash topic-Transfer
    let transfer_topic = Transfer::SIGNATURE_HASH.to_string();
    info!("Targeting Transfer topic: {}", transfer_topic);

    // Shutdown
    let mut shutdown_rx = shutdown_signal().subscribe();

    info!("Starting Decoder Service...");

    loop {
        tokio::select! {
            res = processing_loop(&ch, &pg_pool, &transfer_topic, settings.chain.chain_id) => {
                if let Err(e) = res {
                    error!("Decoder error: {:?}. Retrying...", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
            _ = shutdown_rx.recv() => {
                info!("🛑 Shutting down decoder...");
                break;
            }
        }      
    }

    Ok(())

}

#[derive(sqlx::FromRow)]
struct BlockInfo {
    number: i64,
    hash: String,
}

/// Checks if the latest block has become orphaned
/// Return safe block number to start decoding from
async fn check_cursor_validity(
    pg: &PgPool, 
    chain_id: u64, 
    last_processed_block: i64,
    last_processed_hash: Option<String>
) -> Result<i64> {

    if last_processed_block == 0 {
        return Ok(0);
    }

    // If we dont have previous hash, believe that number or reset
    let Some(last_hash) = last_processed_hash else {
        return Ok(last_processed_block);
    };

    // Check that block's status in canonical_blocks
    let status_row = sqlx::query!(
        r#"
        SELECT status FROM canonical_blocks 
        WHERE chain_id = $1::bigint AND number = $2::bigint AND hash = $3
        "#,
        chain_id as i64,
        last_processed_block,
        last_hash
    )
    .fetch_optional(pg)
    .await?;

    match status_row {
        Some(row) => {
            if row.status == "canonical" {
                // Ok
                Ok(last_processed_block)
            } else {
                // block becomes orphan or finalized
                // If orphan, we need to back
                warn!("🚨 Decoder cursor is on ORPHAN block {} ({}). Rolling back...", last_processed_block, last_hash);

                let valid = sqlx::query!(
                    r#"
                    SELECT number FROM canonical_blocks 
                    WHERE chain_id = $1::bigint AND status = 'canonical' AND number < $2::bigint
                    ORDER BY number DESC
                    LIMIT 1
                    "#,
                    chain_id as i64,
                    last_processed_block
                )
                .fetch_optional(pg)
                .await?;

                let safe_block = valid.map(|r| r.number).unwrap_or(0);
                info!("🔄 Rolled back decoder to block {}", safe_block);
                Ok(safe_block)
            }
        },
        None => {
            // Block is absent in DB?
            // Let back
            warn!("Decoder cursor block not found in DB. Rolling back 100 blocks.");
            Ok((last_processed_block - 100).max(0))
        }
    }
}

/// Loop decoded
async fn processing_loop(ch: &ClickHouseClient, pg: &PgPool, transfer_topic: &str, chain_id: u64) -> Result<()> {
    let state = sqlx::query!(
        r#"SELECT last_processed_block, last_processed_hash FROM decoder_state WHERE id = 'erc20_worker'"#
    )
    .fetch_optional(pg)
    .await?;

    // Find, where we stopped
    let (mut last_processed, last_hash) = match state {
        Some(r) => (r.last_processed_block, r.last_processed_hash.map(|h| h.trim().to_string())),
        None => {
            // Init, if there is no record
            sqlx::query!("INSERT INTO decoder_state (id, last_processed_block) VALUES ('erc20_worker', 0)")
                .execute(pg).await?;
            (0, None)
        }
    };

    // Check for reorg
    let verified_block = check_cursor_validity(pg, chain_id, last_processed, last_hash).await?;
    if verified_block != last_processed {
        // If cursor is changed (there was a rollback), update var and DB
        last_processed = verified_block;
        // Update DB to fix rollback
        sqlx::query!(
            "UPDATE decoder_state SET last_processed_block = $1::bigint, last_processed_hash = NULL WHERE id = 'erc20_worker'",
            last_processed
        )
        .execute(pg)
        .await?;
    }

    // Get tip from DB
    let tip = sqlx::query_as!(
        BlockInfo,
        r#"
        SELECT number, hash
        FROM canonical_blocks
        WHERE chain_id = $1::bigint AND status = 'canonical'
        ORDER BY number DESC
        LIMIT 1
        "#,
        chain_id as i64
    )
    .fetch_optional(pg)
    .await?;

    let Some(tip) = tip else {
        info!("No canonical blocks yet. Sleeping...");
        sleep(Duration::from_secs(2)).await;
        return Ok(());
    };

    let tip_number = tip.number;

    // Metrics
    gauge!("decode_head_block").set(last_processed as f64);
    gauge!("decode_chain_tip").set(tip.number as f64);
    let lag = (tip.number - last_processed).max(0);
    gauge!("decode_lag").set(lag as f64);

    if tip_number <= last_processed {
        sleep(Duration::from_secs(2)).await;
        return Ok(());
    }

    let start_scan_block = last_processed + 1;

    // Get canonical batch from Postgres
    let canonical_batch = sqlx::query_as!(
        BlockInfo,
        r#"
        SELECT number, hash
        FROM canonical_blocks
        WHERE chain_id = $1::bigint
          AND status = 'canonical'
          AND number >= $2::bigint
          AND number <= $3::bigint
        ORDER BY number ASC
        LIMIT $4
        "#,
        chain_id as i64,
        start_scan_block,
        tip.number,
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
        WHERE chain_id = {} AND topic0 = '{}' AND block_hash IN ({})
        ORDER BY block_number ASC
        "#,
        chain_id, transfer_topic, hashes_str
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
            let count = transfers.len() as u64; // Safe count

            let mut insert = ch.insert::<Erc20Transfer>("erc20_transfers_head").await?;
            for row in transfers.iter() { 
                insert.write(row).await?; 
            }
            insert.end().await?;

            counter!("decode_transfers_total").increment(count);

            info!("Upserted {} transfers", count);
        }
    }

    sqlx::query!(
        r#"
        UPDATE decoder_state
        SET last_processed_block = $1::bigint,
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