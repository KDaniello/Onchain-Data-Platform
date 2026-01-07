use anyhow::{Context, Result};
use clickhouse::{Client as ClickHouseClient};
use common::{settings::Settings, db::{connect_ch, connect_pg}};
use dotenv::dotenv;
use sqlx::postgres::PgPool;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, error};

#[derive(sqlx::FromRow)]
struct BlockInfo {
    number: i64,
    hash: String
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let settings = Settings::new().context("Config")?;

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
            .add_directive(tracing::Level::INFO.into()))
        .init();

    info!("Starting finalizer service...");

    let ch = connect_ch(&settings.clickhouse);
    let pg = connect_pg(&settings.database).await?;

    // Depth finalizing or default from config
    let depth = settings.chain.reorg_depth as i64;
    let chain_id = settings.chain.chain_id;
    let batch_size = 1000;

    loop {
        if let Err(e) = run_loop(&ch, &pg, depth, batch_size, chain_id).await {
            error!("Finalizer error: {:?}. Retrying", e);
            sleep(Duration::from_secs(10)).await;
        }
    }
}

async fn run_loop(ch: &ClickHouseClient, pg: &PgPool, depth: i64, batch_size: i64, chain_id: u64) -> Result<()> {
    let state = sqlx::query!(
        "SELECT last_processed_block FROM decoder_state WHERE id = 'finalizer_worker'"
    )
    .fetch_optional(pg)
    .await?;

    let last_finalized = state.map(|r| r.last_processed_block).unwrap_or(0);

    // Which Tip is
    let tip_row = sqlx::query!(
        "SELECT number FROM canonical_blocks WHERE chain_id = $1::bigint AND status = 'canonical' ORDER BY number DESC LIMIT 1",
        chain_id as i64
    )
    .fetch_optional(pg)
    .await?;

    let Some(tip) = tip_row else {
        sleep(Duration::from_secs(5)).await;
        return Ok(());
    };

    let safe_height = tip.number - depth;

    if last_finalized >= safe_height {
        info!("Synced to safe height {}. Waiting for new blocks...", safe_height);
        sleep(Duration::from_secs(10)).await;
        return Ok(());
    }

    let next_end = (last_finalized + batch_size).min(safe_height);

    let blocks = sqlx::query_as!(
        BlockInfo,
        r#"
        SELECT number, hash 
        FROM canonical_blocks 
        WHERE chain_id = $1::bigint 
          AND status = 'canonical' 
          AND number > $2::bigint 
          AND number <= $3::bigint
        ORDER BY number ASC
        "#,
        chain_id as i64,
        last_finalized,
        next_end
    )
    .fetch_all(pg)
    .await?;

    if blocks.is_empty() {
        update_cursor(pg, next_end).await?;
        return Ok(());
    }

    let end_block = blocks.last().unwrap().number;
    let hashes_str = blocks.iter()
        .map(|b| format!("'{}'", b.hash))
        .collect::<Vec<_>>()
        .join(",");

    info!("Finalizing blocks {} -> {} ({} blocks)", blocks[0].number, end_block, blocks.len());

    let query_logs = format!(
        r#"
        INSERT INTO raw_logs_finalized
        SELECT * FROM raw_logs_head
        WHERE chain_id = {} AND block_hash IN ({})
        "#,
        chain_id, hashes_str
    );
    ch.query(&query_logs).execute().await?;

    let query_transfers = format!(
        r#"
        INSERT INTO erc20_transfers_finalized
        SELECT * FROM erc20_transfers_head
        WHERE chain_id = {} AND block_hash IN ({})
        "#,
        chain_id, hashes_str
    );
    ch.query(&query_transfers).execute().await?;

    update_cursor(pg, end_block).await?;

    info!("Finalized up to {}", end_block);

    Ok(())
}

/// Get next block
async fn update_cursor(pg: &PgPool, block_num: i64) -> Result<()> {
    sqlx::query!(
        "UPDATE decoder_state SET last_processed_block = $1::bigint, updated_at = NOW() WHERE id = 'finalizer_worker'",
        block_num
    ).execute(pg).await?;
    Ok(())
}