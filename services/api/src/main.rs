use axum::{
    routing::get,
    Router
};
use clickhouse::Client as ClickHouseClient;
use common::{
    db::{connect_ch, connect_pg}, settings::Settings, shutdown::shutdown_signal};
use dotenv::dotenv;
use std::net::SocketAddr;
use tower_http::trace::TraceLayer;
use tracing::info;
use sqlx::postgres::PgPool;

mod handlers;

#[derive(Clone)]
struct AppState {
    ch: ClickHouseClient,
    pg: PgPool,
    chain_id: u64
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env()
            .add_directive(tracing::Level::INFO.into()))
        .init();
    let settings = Settings::new().expect("Failed to load settings");

    info!("Starting API Services on port 4000...");

    // ClickHouse
    let ch = connect_ch(&settings.clickhouse);
    // PG
    let pg = connect_pg(&settings.database).await.expect("PG Connect");

    let state = AppState { ch, pg, chain_id: settings.chain.chain_id };

    // Router
    let app = Router::new()
        .route("/health", get(handlers::health::health_check))
        .route("/head", get(handlers::head::get_head))
        .route("/transfers", get(handlers::transfers::get_transfers))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    // Start server on port 4000 (to not conflict with Grafana on port 3000)
    let addr = SocketAddr::from(([0, 0, 0, 0], 4000));
    info!("API listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal_wrapper())
        .await
        .unwrap();

    async fn shutdown_signal_wrapper() {
        let mut rx = shutdown_signal().subscribe();
        let _ = rx.recv().await;
        info!("API shutting down...");
    }
}