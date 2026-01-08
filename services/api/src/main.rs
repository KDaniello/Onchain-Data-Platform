use axum::{
    Router, 
    extract::Request, 
    middleware::{self, Next}, 
    response::Response, 
    routing::get
};
use clickhouse::Client as ClickHouseClient;
use common::{
    db::{connect_ch, connect_pg}, settings::Settings, shutdown::shutdown_signal};
use dotenv::dotenv;
use std::net::SocketAddr;
use tower_http::trace::TraceLayer;
use tracing::info;
use sqlx::postgres::PgPool;
use common::metrics::init_metrics;
use std::time::Instant;
use metrics::{counter, histogram};

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

    init_metrics(9093).expect("Metrics init failed"); // 9093 port for API

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
        .layer(middleware::from_fn(track_metrics))
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

async fn track_metrics(req: Request, next: Next) -> Response {

    let start = Instant::now();
    let path = req.uri().path().to_owned();
    let method = req.method().clone();

    // Let response
    let response = next.run(req).await;

    // Measure result
    let latency = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    let labels = [
        ("method", method.to_string()),
        ("path", path),
        ("status", status)
    ];

    counter!("api_requests_total", &labels).increment(1);
    histogram!("api_request_duration_seconds", &labels).record(latency);

    response
}