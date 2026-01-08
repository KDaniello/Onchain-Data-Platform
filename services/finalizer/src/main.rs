use anyhow::{Context, Result};
use clickhouse::{Client as ClickHouseClient};
use common::{db::{connect_ch, connect_pg}, settings::Settings};
use common::shutdown::shutdown_signal;
use dotenv::dotenv;
use sqlx::postgres::PgPool;
use std::time::Duration;
use tracing::{info, error};
use common::metrics::init_metrics;
use metrics::{counter, gauge};

#[derive(sqlx::FromRow)]
struct BlockInfo {
    number: i64,
    hash: String
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    init_metrics(9094)?; // 9094 port for finalizer

    let settings = Settings::new().context("Config")?;

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
            .add_directive(tracing::Level::INFO.into()))
        .init();

    let ch = connect_ch(&settings.clickhouse);
    let pg = connect_pg(&settings.database).await?;

    // Depth finalizing or default from config
    let depth = settings.chain.reorg_depth as i64;
    let chain_id = settings.chain.chain_id;
    let batch_size = 1000;

    let mut shutdown_rx = shutdown_signal().subscribe();

    info!("Starting Finalizer Service (Depth: {} blocks)...", depth);

    loop {
        tokio::select! {
            res = run_loop(&ch, &pg, depth, batch_size, chain_id) => {
                if let Err(e) = res {
                    error!("Finalizer error: {:?}. Retrying...", e);
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }
            _ = shutdown_rx.recv() => {
                info!("🛑 Shutting down finalizer...");
                break;
            }
        }
    }

    Ok(())

    
}

/// Finalizer loop
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
        tokio::time::sleep(Duration::from_secs(5)).await;
        return Ok(());
    };

    // Safety height to finalize
    let safe_height = tip.number - depth;

    // Wait for safe height
    if last_finalized >= safe_height {
        info!("Synced to safe height {}. Waiting for new blocks...", safe_height);
        tokio::time::sleep(Duration::from_secs(10)).await;
        return Ok(());
    }

    // Get blocks' batch to finalize
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
        // Just move cursor
        update_cursor(pg, chain_id, next_end, "").await?;
        return Ok(());
    }

    let end_block = blocks.last().unwrap().number;
    let end_hash = blocks.last().unwrap().hash.clone();

    let hashes_str = blocks.iter()
        .map(|b| format!("'{}'", b.hash))
        .collect::<Vec<_>>()
        .join(",");

    info!("Finalizing blocks {} -> {} ({} blocks)", blocks[0].number, end_block, blocks.len());

    // Copy data from Head to Finalized
    // Copy only data, which block_chain is matched with canonical
    
    // Logs
    let query_logs = format!(
        r#"
        INSERT INTO raw_logs_finalized
        SELECT * FROM raw_logs_head
        WHERE chain_id = {} AND block_hash IN ({})
        "#,
        chain_id, hashes_str
    );
    ch.query(&query_logs).execute().await?;

    // Transfers
    let query_transfers = format!(
        r#"
        INSERT INTO erc20_transfers_finalized
        SELECT * FROM erc20_transfers_head
        WHERE chain_id = {} AND block_hash IN ({})
        "#,
        chain_id, hashes_str
    );
    ch.query(&query_transfers).execute().await?;

    update_cursor(pg, chain_id, end_block, &end_hash).await?;

    gauge!("finalizer_head_block").set(end_block as f64);
    counter!("finalizer_blocks_total").increment(blocks.len() as u64);

    info!("Finalized up to {}", end_block);

    Ok(())
}

/// Get next block
async fn update_cursor(pg: &PgPool, chain_id: u64, block_num: i64, block_hash: &str) -> Result<()> {
    
    let mut tx = pg.begin().await?;
    
    // Worker's cursor
    sqlx::query!(
        r#"
        INSERT INTO decoder_state (id, last_processed_block, last_processed_hash, updated_at)
        VALUES ('finalizer_worker', $1::bigint, $2, NOW())
        ON CONFLICT (id) DO UPDATE SET
            last_processed_block = EXCLUDED.last_processed_block,
            last_processed_hash = EXCLUDED.last_processed_hash,
            updated_at = NOW()
        "#,
        block_num,
        block_hash
    )
    .execute(&mut *tx)
    .await?;

    // Global chain's state (for api)
    if !block_hash.is_empty() {
        sqlx::query!(
            r#"
            INSERT INTO chain_state (chain_id, finalized_number, finalized_hash)
            VALUES ($1::bigint, $2::bigint, $3)
            ON CONFLICT (chain_id) DO UPDATE SET
                finalized_number = EXCLUDED.finalized_number,
                finalized_hash = EXCLUDED.finalized_hash
            "#,
            chain_id as i64,
            block_num,
            block_hash
        )
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}