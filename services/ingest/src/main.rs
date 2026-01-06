use alloy::{
    providers::{Provider, ProviderBuilder},
    rpc::types::{Block, BlockNumberOrTag, Filter}
};
use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use clickhouse::{Client as ClickHouseClient};
use common::models::{BlockStatus, RawLog};
use dotenv::dotenv;
use sqlx::{postgres::{PgPool, PgPoolOptions}};
use std::env;
use tracing::{info, error};

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

    // Get the latest block
    let current_block_number = provider.get_block_number().await?;
    info!("Processing latest block: {}", current_block_number);

    // Get block
    if let Some(block) = provider.get_block_by_number(BlockNumberOrTag::Number(current_block_number)).await? {
        info!("Fetched block: {} (hash: {})", block.header.number, block.header.hash);
    
        // Save to PG
        save_canonical_block(&pg_pool, 1, &block).await?;
        // Save tp CH
        fetch_and_save_logs(&provider, &ch_client, &block).await?;

        info!("Block {} fully inegsted into PG and CH!", current_block_number);
        
    } else {
        error!("Block {} not found on RPC!", current_block_number)
    }

    Ok(()) 
}

// function: load to TABLE canonical_blocks
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

    // Status - canonical
    let status = BlockStatus::Canonical;

    // SQL Query
    sqlx::query!(
        r#"
        INSERT INTO canonical_blocks
        (chain_id, number, hash, parent_hash, block_timestamp, status, inserted_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        ON CONFLICT (chain_id, number) DO UPDATE SET
            hash = EXCLUDED.hash,
            parent_hash = EXCLUDED.parent_hash,
            block_timestamp = EXCLUDED.block_timestamp,
            status = EXCLUDED.status,
            inserted_at = NOW()
        "#,
        chain_id,
        number,
        hash,
        parent_hash,
        timestamp,
        status as BlockStatus
    )
    .execute(pg_pool)
    .await?;

    Ok(())
}

/// Get Logs from eth_getLogs and write batch to CH
async fn fetch_and_save_logs<P>(provider: &P, ch: &ClickHouseClient, block: &Block) -> Result<()> 
where P: Provider
{
    let block_number = block.header.number;
    let timestamp = block.header.timestamp as u32;

    let filter = Filter::new().select(block_number);
    let logs = provider.get_logs(&filter).await?;

    if logs.is_empty() {
        info!("No logs in block {}", block_number);
        return Ok(());
    }

    let logs_count = logs.len();
    let mut batch = Vec::with_capacity(logs_count);

    for log in logs {
        let tx_hash = log.transaction_hash.map(|h| h.to_string()).unwrap_or_default();
        let address = log.address().to_string();
        let topics = log.topics();

        batch.push(RawLog {
            chain_id: 1,
            block_number,
            block_hash: block.header.hash.to_string(),
            tx_hash,
            log_index: log.log_index.unwrap_or(0) as u32,
            address,
            topic0: topics.get(0).map(|t| t.to_string()).unwrap_or_default(),
            topic1: topics.get(1).map(|t| t.to_string()).unwrap_or_default(),
            topic2: topics.get(2).map(|t| t.to_string()).unwrap_or_default(),
            topic3: topics.get(3).map(|t| t.to_string()).unwrap_or_default(),
            data: log.data().data.to_string(),
            block_timestamp: timestamp
        });
    }

    // Batch insert into ClickHouse
    let mut insert = ch.insert::<RawLog>("raw_logs_head").await?;
    for row in batch {
        insert.write(&row).await?;
    }
    insert.end().await?;

    info!("Inserted {} logs into ClickHouse", logs_count);
    Ok(())
}
