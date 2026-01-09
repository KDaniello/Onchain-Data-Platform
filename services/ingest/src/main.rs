#![allow(clippy::collapsible_if)]

mod reorg;
use reorg::{apply_reorg, detect_and_handle_reorg};

use alloy::{
    providers::{Provider, ProviderBuilder},
    rpc::types::{Block, BlockNumberOrTag, Filter},
};
use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use clickhouse::Client as ClickHouseClient;
use common::metrics::init_metrics;
use common::shutdown::shutdown_signal;
use common::{
    db::{connect_ch, connect_pg},
    models::RawLog,
    settings::Settings,
};
use dotenv::dotenv;
use metrics::{counter, gauge, histogram};
use sqlx::postgres::PgPool;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{error, info, warn};

#[derive(sqlx::FromRow)]
struct BlockTip {
    number: i64,
    hash: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // env
    dotenv().ok();

    // Connect to metrics
    init_metrics(9091)?; // 9091 port for ingest

    // Init config
    let settings = Settings::new().context("Failed to load settings")?;

    // Logs
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    info!(
        "Starting Ingest Service for Chain ID: {}",
        settings.chain.chain_id
    );

    // Connect to Postgres
    info!("Connecting to Postgres...");
    let pg_pool = connect_pg(&settings.database).await?;
    info!("Connected to Postgres!");

    // Connect to ClickHouse
    info!("Connecting to ClickHouse...");
    let ch_client = connect_ch(&settings.clickhouse);
    info!("Connected to ClickHouse!");

    // Connect to RPC
    info!("Connecting to RPC: {}", settings.chain.rpc_url);
    let provider = ProviderBuilder::new().connect_http(settings.chain.rpc_url.parse()?);

    info!("All systems go. Starting ingestion loop.");

    let notify_shutdown = shutdown_signal();
    let mut shutdown_rx = notify_shutdown.subscribe();

    loop {
        tokio::select! {
            res = processing_loop(&pg_pool, &ch_client, &provider, &settings) => {
                if let Err(e) = res {
                    error!("Error in ingestion loop: {:?}. Retrying in 5s...", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }

            _ = shutdown_rx.recv() => {
                info!("🛑 Shutting down ingest service gracefully...");
                break;
            }
        }
    }

    info!("Service stopped!");
    Ok(())
}

/// Define next block and get it
async fn processing_loop<P>(
    pool: &PgPool,
    ch: &ClickHouseClient,
    provider: &P,
    settings: &Settings,
) -> Result<()>
where
    P: Provider,
{
    // Start to measure time
    let start_time = Instant::now();

    let chain_id = settings.chain.chain_id;

    // Get local Head (Canonical) from Postgres -> (number, hash)
    let local_tip = get_canonical_tip(pool, chain_id).await?;

    // If DB is empty start with hard-num. Otherwise take Tip + 1
    let target_number = local_tip
        .as_ref()
        .map(|(n, _)| n + 1)
        .unwrap_or(settings.chain.start_block);

    // Get next chain
    let chain_tip = provider.get_block_number().await?;

    gauge!("ingest_head_block").set(target_number as f64);
    gauge!("ingest_chain_tip").set(chain_tip as f64);

    // Lag
    let lag = (chain_tip as i64 - target_number as i64).max(0);
    gauge!("ingest_lag").set(lag as f64);

    if target_number > chain_tip {
        gauge!("ingest_lag").set(0.0); // Lag is absent
        info!(
            "Synced at block {}. Waiting for new blocks...",
            target_number - 1
        );
        sleep(Duration::from_secs(12)).await;
        return Ok(());
    }

    // How far behind we are
    info!("Processing block {}", target_number);

    // Get block
    if let Some(block) = provider
        .get_block_by_number(BlockNumberOrTag::Number(target_number))
        .await?
    {
        let parent_hash = block.header.parent_hash.to_string();

        // Reorg check
        if let Some((tip_num, tip_hash)) = local_tip {
            if parent_hash != tip_hash {
                warn!(
                    "⚠️ REORG DETECTED at block {}! RPC parent {} != Local tip {}. Rolling back block {}...",
                    target_number, parent_hash, tip_hash, tip_num
                );

                // Let find Reorg blocks
                let reorg_result = detect_and_handle_reorg(
                    pool,
                    provider,
                    chain_id,
                    &block,
                    tip_num as i64,
                    tip_hash,
                )
                .await?;

                // Apply to DB
                apply_reorg(pool, chain_id, &reorg_result, &block).await?;

                return Ok(());
            }
        }

        // Save to PG
        save_canonical_block(pool, chain_id, &block).await?;
        // Update Global state
        update_chain_state(pool, chain_id, &block).await?;
        // Save tp CH
        fetch_and_save_logs(provider, ch, &block, chain_id).await?;

        counter!("ingest_blocks_processed_total").increment(1);
        histogram!("ingest_block_duration_seconds").record(start_time.elapsed().as_secs_f64());
    } else {
        warn!("Block {} missing", target_number);
        sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}

/// Get canonical the latest block -> (number, hash)
async fn get_canonical_tip(pool: &PgPool, chain_id: u64) -> Result<Option<(u64, String)>> {
    let row = sqlx::query_as!(
        BlockTip,
        r#"
        SELECT number, hash::text as "hash!"
        FROM canonical_blocks 
        WHERE chain_id = $1::bigint AND status = 'canonical' 
        ORDER BY number DESC 
        LIMIT 1
        "#,
        chain_id as i64
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| (r.number as u64, r.hash)))
}

/// function: load to TABLE canonical_blocks
async fn save_canonical_block(pg_pool: &PgPool, chain_id: u64, block: &Block) -> Result<()> {
    // u64 to i64
    let number = block.header.number as i64;
    let chain_id_i64 = chain_id as i64;

    // to string
    let hash = block.header.hash.to_string();
    let parent_hash = block.header.parent_hash.to_string();

    // to DateTime<Utc>
    let timestamp = Utc
        .timestamp_opt(block.header.timestamp as i64, 0)
        .single()
        .context("Invalid timestamp")?;

    let mut tx = pg_pool.begin().await?;

    // Remove canonical status from any blocks
    sqlx::query!(
        r#"
        UPDATE canonical_blocks
        SET status = 'orphan'
        WHERE chain_id = $1::bigint AND number = $2::bigint AND status = 'canonical' AND hash != $3
        "#,
        chain_id_i64,
        number,
        hash
    )
    .execute(&mut *tx)
    .await?;

    // If block was orphan, let it to canonical back
    sqlx::query!(
        r#"
        INSERT INTO canonical_blocks (chain_id, number, hash, parent_hash, block_timestamp, status, inserted_at)
        VALUES ($1::bigint, $2::bigint, $3, $4, $5, 'canonical', NOW())
        ON CONFLICT (chain_id, hash) DO UPDATE SET
            status = 'canonical',
            parent_hash = EXCLUDED.parent_hash,
            inserted_at = NOW()
        "#,
        chain_id_i64,
        number,
        hash,
        parent_hash,
        timestamp
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(())
}

/// Get Logs from eth_getLogs and write batch to CH
async fn fetch_and_save_logs<P>(
    provider: &P,
    ch: &ClickHouseClient,
    block: &Block,
    chain_id: u64,
) -> Result<()>
where
    P: Provider,
{
    let filter = Filter::new().at_block_hash(block.header.hash);
    let logs = provider.get_logs(&filter).await?;

    if logs.is_empty() {
        return Ok(());
    }

    let now_ms = Utc::now().timestamp_millis() as u64;
    let mut batch = Vec::with_capacity(logs.len());

    for log in logs {
        batch.push(RawLog {
            chain_id,
            block_number: block.header.number,
            block_hash: block.header.hash.to_string(),
            tx_hash: log
                .transaction_hash
                .map(|h| h.to_string())
                .unwrap_or_default(),
            log_index: log.log_index.unwrap_or(0) as u32,
            address: log.address().to_string().to_lowercase(),
            topic0: log
                .topics()
                .first()
                .map(|t| t.to_string())
                .unwrap_or_default(),
            topic1: log
                .topics()
                .get(1)
                .map(|t| t.to_string())
                .unwrap_or_default(),
            topic2: log
                .topics()
                .get(2)
                .map(|t| t.to_string())
                .unwrap_or_default(),
            topic3: log
                .topics()
                .get(3)
                .map(|t| t.to_string())
                .unwrap_or_default(),
            data: log.data().data.to_string(),
            block_timestamp: block.header.timestamp as u32,
            inserted_at: now_ms,
        });
    }

    let batch_len = batch.len() as u64;

    // Batch insert into ClickHouse
    let mut insert = ch.insert::<RawLog>("raw_logs_head").await?;
    for row in batch {
        insert.write(&row).await?;
    }
    insert.end().await?;

    counter!("ingest_logs_total").increment(batch_len);
    Ok(())
}

async fn update_chain_state(pool: &PgPool, chain_id: u64, block: &Block) -> Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO chain_state (chain_id, head_number, head_hash, updated_at)
        VALUES ($1::bigint, $2::bigint, $3, NOW())
        ON CONFLICT (chain_id) DO UPDATE SET
            head_number = EXCLUDED.head_number,
            head_hash = EXCLUDED.head_hash,
            updated_at = NOW()
        "#,
        chain_id as i64,
        block.header.number as i64,
        block.header.hash.to_string()
    )
    .execute(pool)
    .await?;
    Ok(())
}
