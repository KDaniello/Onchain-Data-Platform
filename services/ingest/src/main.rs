use alloy::{
    providers::{Provider, ProviderBuilder},
    rpc::types::{Block, BlockNumberOrTag}
};
use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use common::models::BlockStatus;
use dotenv::dotenv;
use sqlx::postgres::{PgPool, PgPoolOptions};
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

    let chain_id: i64 = 1;

    // Connect to Postgres
    info!("Connecting to Postgres...");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .context("Failed to connect to Postgres")?;
    info!("Connected to Postgres!");

    // Connect to RPC
    info!("Connecting to RPC: {}", rpc_url);
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);

    // Test request - Get number of the latest block
    let current_block_number = provider.get_block_number().await?;
    info!("Latest Block number: {}", current_block_number);

    // Get block
    if let Some(block) = provider.get_block_by_number(BlockNumberOrTag::Number(current_block_number)).await? {
        info!("Fetched block: {} (hash: {})", block.header.number, block.header.hash);
    
        save_canonical_block(&pool, chain_id, &block).await?;
        info!("Block successfully saved to Postgres!")
    } else {
        error!("Block {} not found!", current_block_number)
    }

    Ok(()) 
}

// function: load to TABLE canonical_blocks
async fn save_canonical_block(pool: &PgPool, chain_id: i64, block:&Block) -> Result<()> {

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
    .execute(pool)
    .await?;

    Ok(())
}
