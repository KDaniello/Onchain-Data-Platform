use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Json, Router
};
use clickhouse::{Client as ClickHouseClient};
use common::models::Erc20Transfer;
use dotenv::dotenv;
use serde::{Deserialize};
use std::{
    env, net::SocketAddr
};
use tower_http::trace::{TraceLayer};
use tracing::{error, info};
use sqlx::postgres::{PgPool, PgPoolOptions};

#[derive(Clone)]
struct AppState {
    ch: ClickHouseClient,
    pg: PgPool
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
            .add_directive(tracing::Level::INFO.into()))
        .init();

    info!("Starting API Services...");

    // Config ClickHouse
    let ch_url = "http://localhost:8123";

    let ch = ClickHouseClient::default()
        .with_url(ch_url)
        .with_database("onchain_data");

    // Config PG
    let db_url = env::var("DATABASE_URL").expect("DB URL set");
    let pg = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .unwrap();

    let state = AppState { ch, pg };

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

    let canonical_hashes = sqlx::query!(
        "SELECT hash FROM canonical_blocks WHERE chain_id = 1 AND status = 'canonical' ORDER BY number DESC LIMIT 1000"
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
        SELECT * FROM erc20_transfers 
        WHERE {} 
          AND block_hash IN ({}) 
        ORDER BY block_number DESC, log_index DESC 
        LIMIT {}
        "#, 
        base_query, hashes_str, limit
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