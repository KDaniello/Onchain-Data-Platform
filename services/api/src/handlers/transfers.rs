use crate::AppState;
use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use common::models::Erc20Transfer;
use serde::Deserialize;
use tracing::error;

#[derive(Deserialize)]
pub struct TransferParams {
    token: Option<String>,
    limit: Option<u64>,
}

#[derive(sqlx::FromRow)]
struct BlockHash {
    hash: String,
}

pub async fn get_transfers(
    State(state): State<AppState>,
    Query(params): Query<TransferParams>,
) -> Result<Json<Vec<Erc20Transfer>>, (StatusCode, String)> {
    let limit = params.limit.unwrap_or(50).min(1000); // Min 1000 records

    let canonical_hashes = sqlx::query_as!(
        BlockHash,
        r#"
        SELECT hash::text as "hash!" -- Приводим к text, чтобы sqlx точно понял тип
        FROM canonical_blocks 
        WHERE chain_id = $1::bigint AND status = 'canonical' 
        ORDER BY number DESC 
        LIMIT 100
        "#,
        state.chain_id as i64
    )
    .fetch_all(&state.pg)
    .await
    .map_err(|e| {
        error!("PG Error fetching canonical blocks: {:?}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "PG Error".to_string())
    })?;

    if canonical_hashes.is_empty() {
        return Ok(Json(vec![]));
    }

    let hashes_str = canonical_hashes
        .iter()
        .map(|r| format!("'{}'", r.hash))
        .collect::<Vec<_>>()
        .join(",");

    let base_query = if let Some(token) = params.token {
        let clean = token.trim().to_lowercase();
        // Validation eth address
        if !clean.starts_with("0x") || clean.len() != 42 || hex::decode(&clean[2..]).is_err() {
            return Err((StatusCode::BAD_REQUEST, "Invalid token".to_string()));
        }
        format!("token_address = '{}'", clean)
    } else {
        "1=1".to_string()
    };

    let query = format!(
        r#"
        SELECT * FROM erc20_transfers_head 
        WHERE chain_id = {} 
          AND {} 
          AND block_hash IN ({}) 
        ORDER BY block_number DESC, log_index DESC 
        LIMIT {}
        "#,
        state.chain_id, base_query, hashes_str, limit
    );

    let transfers: Vec<Erc20Transfer> = state.ch.query(&query).fetch_all().await.map_err(|e| {
        error!("CH Error: {:?}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, "CH Error".to_string())
    })?;

    Ok(Json(transfers))
}
