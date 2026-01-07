use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Json, Router
};
use clickhouse::{Client as ClickHouseClient};
use common::{models::Erc20Transfer, settings::Settings, db::{connect_ch, connect_pg}};
use dotenv::dotenv;
use serde::{Deserialize};
use std::{net::SocketAddr};
use tower_http::trace::{TraceLayer};
use tracing::{error, info};
use sqlx::postgres::{PgPool};

#[derive(Clone)]
struct AppState {
    ch: ClickHouseClient,
    pg: PgPool,
    chain_id: u64
}

#[derive(sqlx::FromRow)]
struct BlockHash {
    hash: String,
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
            .add_directive(tracing::Level::INFO.into()))
        .init();
    let settings = Settings::new().expect("Failed to load settings");

    info!("Starting API Services...");

    // ClickHouse
    let ch = connect_ch(&settings.clickhouse);
    // PG
    let pg = connect_pg(&settings.database).await.expect("PG Connect");

    let state = AppState { ch, pg, chain_id: settings.chain.chain_id };

    // Router
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/transfers", get(get_transfers))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    // Start server on port 4000 (to not conflict with Grafana on port 3000)
    let addr = SocketAddr::from(([0, 0, 0, 0], 4000));
    info!("API listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// Handlers

async fn health_check() -> &'static str { "Ok" }

#[derive(Deserialize)]
struct TranserParams {
    token: Option<String>,
    limit: Option<u64>
}

async fn get_transfers(
    State(state): State<AppState>,
    Query(params): Query<TranserParams>,
) -> Result<Json<Vec<Erc20Transfer>>, (StatusCode, String)> {

    let limit = params.limit.unwrap_or(50).min(100); // Min 1000 records

    let canonical_hashes = sqlx::query_as!(
        BlockHash,
        "SELECT hash FROM canonical_blocks WHERE chain_id = $1::bigint AND status = 'canonical' ORDER BY number DESC LIMIT 100",
        state.chain_id as i64
    )
    .fetch_all(&state.pg)
    .await
    .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "PG Error".to_string()))?;

    if canonical_hashes.is_empty() {
        return Ok(Json(vec![]));
    }

    let hashes_str = canonical_hashes.iter()
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
        WHERE chain_id = {} AND {} 
          AND block_hash IN ({}) 
        ORDER BY block_number DESC, log_index DESC 
        LIMIT {}
        "#, 
        state.chain_id, base_query, hashes_str, limit
    );

    let transfers: Vec<Erc20Transfer> = state.ch
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| {
            error!("CH Error: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "CH Error".to_string())
        })?;

    Ok(Json(transfers))
}