use alloy::{
    providers::{Provider, ProviderBuilder}, 
    rpc::types::{Block, BlockNumberOrTag, Filter}
};
use anyhow::{Context, Ok, Result};
use chrono::{TimeZone, Utc};
use clickhouse::{Client as ClickHouseClient};
use common::models::{RawLog};
use dotenv::dotenv;
use sqlx::{postgres::{PgPool, PgPoolOptions}};
use std::env;
use tracing::{info, error, warn};
use std::time::Duration;
use std::net::SocketAddr;
use tokio::time::sleep;
use metrics::{counter, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    // env
    dotenv().ok();

    // Logs
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
        )
        .init();

    info!("Starting Ingest Service...");
    
    // Prometheus Build (Metrics)
    let builder = PrometheusBuilder::new();
    let addr: SocketAddr = "0.0.0.0:9091".parse()?;
    builder
        .with_http_listener(addr)
        .install()
        .context("Failed to install Prometheus recorder")?;

    info!("Metrics server running at http://0.0.0.0:9091/metrics");

    // DB url
    let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://admin:admin@localhost:5432/onchain_data".to_string());
    // rpc from env
    let rpc_url = env::var("RPC_HTTP_URL").expect("RPC_HTTP_URL must be set");

    // ClickHouse Settings
    let ch_url = "http://localhost:8123";
    let ch_user = env::var("CH_USER").unwrap_or("default".to_string());
    let ch_pass = env::var("CH_PASSWORD").unwrap_or("".to_string());

    // Connect to Postgres
    info!("Connecting to Postgres...");
    let pg_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .context("Failed to connect to Postgres")?;
    info!("Connected to Postgres!");

    // Connect to ClickHouse
    info!("Connecting to ClickHouse...");
    let ch_client = ClickHouseClient::default()
        .with_url(ch_url)
        .with_user(&ch_user)
        .with_password(&ch_pass)
        .with_database("onchain_data");
    info!("Connected to ClickHouse!");

    // Connect to RPC
    info!("Connecting to RPC: {}", rpc_url);
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);

    info!("All systems go. Starting ingestion loop.");

    loop {
        if let Err(e) = processing_loop(&pg_pool, &ch_client, &provider).await {
            error!("Error in ingestion loop {:?}. Retrying in 5s...", e);
            sleep(Duration::from_secs(5)).await;
        }
    }
}

/// Define next block and get it
async fn processing_loop<P>(pool: &PgPool, ch: &ClickHouseClient, provider: &P) -> Result<()>
where P: Provider
{
    // Get local Head (Canonical) from Postgres -> (number, hash)
    let local_tip = get_canonical_tip(pool).await?;

    // If DB is empty start with hard-num. Otherwise take Tip + 1
    let target_number = local_tip.as_ref().map(|(n, _)| n + 1).unwrap_or(24176840); 

    // Get next chain
    let chain_tip = provider.get_block_number().await?;
    gauge!("ingest_head_block").set(target_number as f64);
    gauge!("chain_tip_block").set(chain_tip as f64);

    if target_number > chain_tip {
        gauge!("ingest_lag").set(0.0); // Lag is absent
        info!("Synced at block {}. Waiting for new blocks...", target_number - 1);
        sleep(Duration::from_secs(12)).await;
        return Ok(());
    }
    
    // How far behind we are
    gauge!("ingest_lag").set((chain_tip - target_number) as f64);
    info!("Processing block {}", target_number);

    // Get block
    if let Some(block) = provider.get_block_by_number(BlockNumberOrTag::Number(target_number)).await? {
        
        let parent_hash = block.header.parent_hash.to_string();

        // Reorg check
        if let Some((tip_num, tip_hash)) = local_tip {
            if parent_hash != tip_hash {
                warn!("⚠️ REORG DETECTED at block {}! RPC parent {} != Local tip {}. Rolling back block {}...",
                target_number, parent_hash, tip_hash, tip_num);

                mark_block_orphan(pool, tip_num, &tip_hash).await?;

                return Ok(());
            }
        }
        
        // Save to PG
        save_canonical_block(pool, 1, &block).await?;
        // Save tp CH
        fetch_and_save_logs(provider, ch, &block).await?;

        counter!("ingest_blocks_processed_total").increment(1);
    } else {
        warn!("Block {} missing", target_number);
        sleep(Duration::from_secs(1)).await;
    }

    Ok(()) 
}

/// Get canonical the latest block -> (number, hash)
async fn get_canonical_tip(pool: &PgPool) -> Result<Option<(u64, String)>> {
    let row = sqlx::query!(
        r#"SELECT number, hash FROM canonical_blocks WHERE chain_id = 1 AND status = 'canonical' ORDER BY number DESC LIMIT 1"#
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| (r.number as u64, r.hash)))
} 

/// Mark block as orphan (leave from canonical chain)
async fn mark_block_orphan(pool: &PgPool, number: u64, hash: &str) -> Result<()> {
    sqlx::query!(
        "UPDATE canonical_blocks SET status = 'orphan' WHERE chain_id = 1 AND number = $1 AND hash = $2",
        number as i64, hash
    )
    .execute(pool)
    .await?;

    counter!("ingest_reorgs_total").increment(1);
    info!("Block {} ({}) marked as Orphan", number, hash);
    Ok(())
}

/// function: load to TABLE canonical_blocks
async fn save_canonical_block(pg_pool: &PgPool, chain_id: i64, block:&Block) -> Result<()> {

    // u64 to i64
    let number = block.header.number as i64;

    // to string
    let hash = block.header.hash.to_string();
    let parent_hash = block.header.parent_hash.to_string();

    // to DateTime<Utc>
    let timestamp = Utc.timestamp_opt(block.header.timestamp as i64, 0)
        .single()
        .context("Invalid timestamp")?;

    let mut tx = pg_pool.begin().await?;

    // Remove canonical status from any blocks
    sqlx::query!(
        r#"
        UPDATE canonical_blocks
        SET status = 'orphan'
        WHERE chain_id = $1 AND number = $2 AND status = 'canonical' AND hash != $3
        "#,
        chain_id,
        number,
        hash
    )
    .execute(&mut *tx)
    .await?;

    // If block was orphan, let it to canonical back 
    sqlx::query!(
        r#"
        INSERT INTO canonical_blocks (chain_id, number, hash, parent_hash, block_timestamp, status, inserted_at)
        VALUES ($1, $2, $3, $4, $5, 'canonical', NOW())
        ON CONFLICT (chain_id, hash) DO UPDATE SET
            status = 'canonical', -- Восстанавливаем статус
            parent_hash = EXCLUDED.parent_hash,
            inserted_at = NOW()
        "#,
        chain_id,
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
async fn fetch_and_save_logs<P>(provider: &P, ch: &ClickHouseClient, block: &Block) -> Result<()> 
where P: Provider
{

    let filter = Filter::new().at_block_hash(block.header.hash);
    let logs = provider.get_logs(&filter).await?;

    if logs.is_empty() {
        return Ok(());
    }

    let mut batch = Vec::with_capacity(logs.len());

    for log in logs {
        batch.push(RawLog {
            chain_id: 1,
            block_number: block.header.number,
            block_hash: block.header.hash.to_string(),
            tx_hash: log.transaction_hash.map(|h| h.to_string()).unwrap_or_default(),
            log_index: log.log_index.unwrap_or(0) as u32,
            address: log.address().to_string().to_lowercase(),
            topic0: log.topics().get(0).map(|t| t.to_string()).unwrap_or_default(),
            topic1: log.topics().get(1).map(|t| t.to_string()).unwrap_or_default(),
            topic2: log.topics().get(2).map(|t| t.to_string()).unwrap_or_default(),
            topic3: log.topics().get(3).map(|t| t.to_string()).unwrap_or_default(),
            data: log.data().data.to_string(),
            block_timestamp: block.header.timestamp as u32
        });
    }

    let batch_len = batch.len() as u64;

    // Batch insert into ClickHouse
    let mut insert = ch.insert::<RawLog>("raw_logs_head").await?;
    for row in batch {
        insert.write(&row).await?;
    }
    insert.end().await?;

    counter!("ingest_logs_total").increment(batch_len as u64);
    Ok(())
}