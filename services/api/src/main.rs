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

#[derive(Clone)]
struct AppState {
    ch: ClickHouseClient
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
    let ch_user = env::var("CH_USER").unwrap_or("default".to_string());
    let ch_pass = env::var("CH_PASSWORD").unwrap_or("".to_string());

    let ch = ClickHouseClient::default()
        .with_url(ch_url)
        .with_user(&ch_user)
        .with_password(&ch_pass)
        .with_database("onchain_data");

    let state = AppState { ch };

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

async fn health_check() -> &'static str {
    "Ok"
}

#[derive(Deserialize)]
struct TranserParams {
    token: Option<String>,
    limit: Option<u64>
}

async fn get_transfers(
    State(state): State<AppState>,
    Query(params): Query<TranserParams>,
) -> Result<Json<Vec<Erc20Transfer>>, (StatusCode, String)> {

    let limit = params.limit.unwrap_or(50).min(1000); // Min 1000 records

    let query = if let Some(token) = params.token {

        // Validation eth address
        let clean_token = token.trim();
        if !clean_token.starts_with("0x") || clean_token.len() != 42 || hex::decode(&clean_token[2..]).is_err() {
            return Err((StatusCode::BAD_REQUEST, "Invalid token address format".to_string()));
        }

        // Normalize: lowercase
        let safe_token = clean_token.to_lowercase();

        format!(
            "SELECT * FROM erc20_transfers WHERE token_address = '{}' ORDER BY block_number DESC, log_index DESC LIMIT {}",
            safe_token, limit)
    } else {
        format!(
            "SELECT * FROM erc20_transfers ORDER BY block_number DESC, log_index DESC LIMIT {}",
            limit)
    };

    let transfers: Vec<Erc20Transfer> = state.ch
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| {
            error!("ClickHouse error: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Database error".to_string())
        })?;

    Ok(Json(transfers))
}