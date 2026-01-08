use alloy::{
    providers::{Provider}, rpc::types::{Block}
};
use anyhow::{Ok, Result, anyhow};
use sqlx::PgPool;
use tracing::{info};

// Results of analysis of reorgs
pub struct ReorgResult {
    pub lca_number: i64,
    pub lca_hash: String,
    pub depth: u32,
    pub orphaned_blocks: Vec<(i64, String)>
}

const MAX_REORG_DEPTH: u32 = 128;

/// Main detection's function
/// Compare local base with data from rpc, finding point of difference
pub async fn detect_and_handle_reorg<P>(
    pool: &PgPool, 
    provider: &P, 
    chain_id: u64, 
    new_block: &Block, 
    local_tip_num: i64, 
    local_tip_hash: String) -> Result<ReorgResult>
where P: Provider
{
    info!("🕵️ Starting reorg detection. Local tip: {} ({}), Remote parent wants: {}", 
        local_tip_num, local_tip_hash, new_block.header.parent_hash);

    let mut depth = 0u32;
    
    // Let check with parent new block (from RPC)
    let mut check_hash = new_block.header.parent_hash.to_string();
    let mut check_number = (new_block.header.number - 1) as i64;

    // Init list of orphaned blocks, which are canonical in base
    let mut orphaned = Vec::new();

    // Add current tip in list orphaned immediately
    orphaned.push((local_tip_num, local_tip_hash));

    loop {
        depth += 1;
        if depth > MAX_REORG_DEPTH {
            return Err(anyhow!("🚨 Reorg too deep (> {}). Manual intervention required.", MAX_REORG_DEPTH));
        }

        let remote_block = provider
            .get_block_by_hash(check_hash.parse()?)
            .await?
            .ok_or_else(|| anyhow!("Critical: Could not fetch block {} from RPC during reorg analysis", check_hash))?;

        let local_block = sqlx::query!(
            r#"
            SELECT hash 
            FROM canonical_blocks 
            WHERE chain_id = $1::bigint AND number = $2::bigint AND status = 'canonical'
            "#,
            chain_id as i64,
            check_number
        )
        .fetch_optional(pool)
        .await?;

        match local_block {
            Some(row) => {
                if row.hash == check_hash {
                    info!("✅ LCA found at block {} ({})", check_number, check_hash);

                    let blocks_to_oprhan = sqlx::query!(
                        r#"
                        SELECT number, hash 
                        FROM canonical_blocks 
                        WHERE chain_id = $1::bigint AND status = 'canonical' AND number > $2::bigint
                        ORDER BY number DESC
                        "#,
                        chain_id as i64,
                        check_number
                    )
                    .fetch_all(pool)
                    .await?;

                    let orphaned_result: Vec<(i64, String)> = blocks_to_oprhan
                        .into_iter()
                        .map(|b| (b.number, b.hash))
                        .collect();

                    return Ok(ReorgResult {
                        lca_number: check_number,
                        lca_hash: check_hash,
                        depth,
                        orphaned_blocks: orphaned_result
                    });
                } else {
                    check_hash = remote_block.header.parent_hash.to_string();
                    check_number -= 1;
                }
            },
            None => {
                check_hash = remote_block.header.parent_hash.to_string();
                check_number -= 1;
            }
        }

        if check_number < 0 {
            return Err(anyhow!("Reached genesis during reorg scan without finding LCA!"));
        }
    }
}

pub async fn apply_reorg(
    pool: &PgPool,
    chain_id: u64,
    reorg: &ReorgResult,
    new_tip_block: &Block
) -> Result<()> {
    let mut tx = pool.begin().await?;

    info!("🔄 Applying reorg: Orphan {} blocks > LCA {}", reorg.orphaned_blocks.len(), reorg.lca_number);

    for (_num, hash) in &reorg.orphaned_blocks {
        sqlx::query!(
            "UPDATE canonical_blocks SET status = 'orphan' WHERE chain_id = $1::bigint AND hash = $2",
            chain_id as i64,
            hash
        )
        .execute(&mut *tx)
        .await?;
    }

    let old_tip = reorg.orphaned_blocks.first();
    let (old_num, old_hash) = match old_tip {
        Some((n, h)) => (*n, h.as_str()),
        None => (0, "")
    };

    sqlx::query!(
        r#"
        INSERT INTO reorg_audit 
        (chain_id, old_head_number, old_head_hash, new_head_number, new_head_hash, 
         lca_number, lca_hash, depth, blocks_orphaned, detected_at)
        VALUES ($1::bigint, $2::bigint, $3, $4::bigint, $5, $6::bigint, $7, $8, $9, NOW())
        "#,
        chain_id as i64,
        old_num,
        old_hash,
        new_tip_block.header.number as i64,
        new_tip_block.header.hash.to_string(),
        reorg.lca_number,
        reorg.lca_hash,
        reorg.depth as i32,
        reorg.orphaned_blocks.len() as i32
    )
    .execute(&mut *tx)
    .await?;

    sqlx::query!(
        r#"
        UPDATE chain_state 
        SET head_number = $2::bigint, head_hash = $3, updated_at = NOW()
        WHERE chain_id = $1::bigint
        "#,
        chain_id as i64,
        reorg.lca_number,
        reorg.lca_hash
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    info!("✅ Reorg applied successfully. New DB head is LCA: {}", reorg.lca_number);

    Ok(())
}