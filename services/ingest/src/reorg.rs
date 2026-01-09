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
pub async fn detect_and_handle_reorg<P, T>(
    pool: &PgPool, 
    provider: &P, 
    chain_id: u64, 
    new_block: &Block<T>, 
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

pub async fn apply_reorg<T>(
    pool: &PgPool,
    chain_id: u64,
    reorg: &ReorgResult,
    new_tip_block: &Block<T>
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

/// Tests
#[cfg(test)]
mod tests {
    use super::*;
    use common::{db::connect_pg, settings::Settings};
    use std::str::FromStr;
    use alloy::primitives::B256;
    use sqlx::postgres::PgPool;

    // Хелпер для создания фейкового блока (заголовка)
    fn create_mock_block(number: u64, hash: &str, parent: &str) -> Block<B256> {
        let mut block: Block<B256> = Block::default();
        block.header.number = number;
        block.header.hash = B256::from_str(hash).unwrap();
        block.header.parent_hash = B256::from_str(parent).unwrap();
        block.header.timestamp = 1234567890;
        block
    }

    // Хелпер для вставки блока в БД напрямую
    async fn insert_block(pool: &PgPool, chain_id: u64, number: i64, hash: &str, status: &str) {
        sqlx::query!(
            r#"
            INSERT INTO canonical_blocks (chain_id, number, hash, parent_hash, block_timestamp, status)
            VALUES ($1, $2, $3, $4, NOW(), $5)
            "#,
            chain_id as i64,
            number,
            hash,
            "0x0000000000000000000000000000000000000000000000000000000000000000", // dummy parent
            status
        )
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_apply_reorg_db_logic() {
        // 1. Setup
        dotenv::dotenv().ok();
        let settings = Settings::new().expect("Config");
        let pool = connect_pg(&settings.database).await.expect("DB Connect");
        
        let chain_id = 99999; 

        // Чистим хвосты от прошлых тестов
        sqlx::query!("DELETE FROM canonical_blocks WHERE chain_id = $1", chain_id)
            .execute(&pool).await.unwrap();
        sqlx::query!("DELETE FROM reorg_audit WHERE chain_id = $1", chain_id)
            .execute(&pool).await.unwrap();
        sqlx::query!("DELETE FROM chain_state WHERE chain_id = $1", chain_id)
            .execute(&pool).await.unwrap();

        // 2. Prepare Data: Chain 100 -> 101
        let hash_100 = "0x0000000000000000000000000000000000000000000000000000000000000100";
        let hash_101 = "0x0000000000000000000000000000000000000000000000000000000000000101";
        
        insert_block(&pool, chain_id as u64, 100, hash_100, "canonical").await;
        insert_block(&pool, chain_id as u64, 101, hash_101, "canonical").await;

        // Мы должны создать начальное состояние chain_state, чтобы apply_reorg мог его обновить
        sqlx::query!(
            "INSERT INTO chain_state (chain_id, head_number, head_hash) VALUES ($1::bigint, 101, $2)",
            chain_id as i64,
            hash_101
        )
        .execute(&pool)
        .await
        .unwrap();

        // 3. Simulate Reorg
        // Допустим, мы поняли, что 101 - плохой. LCA = 100.
        // Новый блок 101_NEW (который пришел из RPC)
        let hash_101_new = "0x0000000000000000000000000000000000000000000000000000000000000102";
        let new_tip_block = create_mock_block(101, hash_101_new, hash_100);

        let reorg_result = ReorgResult {
            lca_number: 100,
            lca_hash: hash_100.to_string(),
            depth: 1,
            orphaned_blocks: vec![(101, hash_101.to_string())],
        };

        // 4. Action
        apply_reorg(&pool, chain_id as u64, &reorg_result, &new_tip_block)
            .await
            .expect("Apply reorg failed");

        // 5. Verify

        // A. Блок 101 должен стать ORPHAN
        let status_opt = sqlx::query!(
            "SELECT status FROM canonical_blocks WHERE chain_id = $1::bigint AND hash = $2",
            chain_id as i64, 
            hash_101
        )
        .fetch_optional(&pool)
        .await
        .unwrap();

        if status_opt.is_none() {
            panic!("❌ Block 101 not found in DB! Check insert logic.");
        }
        assert_eq!(status_opt.unwrap().status, "orphan");

        // B. Блок 100 должен остаться CANONICAL
        let status_100 = sqlx::query!(
            "SELECT status FROM canonical_blocks WHERE chain_id = $1::bigint AND hash = $2",
            chain_id as i64, 
            hash_100
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(status_100.status, "canonical");

        // C. Chain State
        let state = sqlx::query!(
            "SELECT head_number FROM chain_state WHERE chain_id = $1::bigint", 
            chain_id as i64
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        
        // Теперь здесь будет 100, потому что запись была создана и успешно обновлена
        assert_eq!(state.head_number, 100);

        // D. Audit log должен быть записан
        let audit = sqlx::query!("SELECT * FROM reorg_audit WHERE chain_id = $1", chain_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(audit.depth, 1);
        assert_eq!(audit.lca_number, 100);

        println!("✅ Test Passed: Reorg logic correctly orphaned block 101 and updated state.");
    }
}