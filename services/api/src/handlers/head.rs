use axum::{extract::State, Json};
use common::models::ChainState;
use crate::AppState;
use serde::Serialize;

#[derive(Serialize)]
pub struct HeadResponse {
    pub chain_id: i64,
    pub head_number: i64,
    pub head_hash: String,
    pub finalized_number: i64,
    pub lag_blocks: i64,
    pub updated_at: String
}

pub async fn get_head(State(state): State<AppState>) -> Json<Option<HeadResponse>> {
    let db_state = sqlx::query_as!(
        ChainState,
        "SELECT * FROM chain_state WHERE chain_id = $1",
        state.chain_id as i64
    )
    .fetch_optional(&state.pg)
    .await
    .unwrap_or(None);

    let response = db_state.map(|s| HeadResponse {
        chain_id: s.chain_id,
        head_number: s.head_number,
        head_hash: s.head_hash,
        finalized_number: s.finalized_number,
        lag_blocks: 0, // TODO: Implement RPC check
        updated_at: s.updated_at.map(|t| t.to_rfc3339()).unwrap_or_default()
    });

    Json(response)
}