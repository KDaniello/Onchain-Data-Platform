use anyhow::Result;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use tracing::info;

/// Run HTTP server for Prometheus on specified port
/// Metrics would be available at the address http://0.0.0.0:PORT/metrics
pub fn init_metrics(port: u16) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let builder = PrometheusBuilder::new();
    builder.with_http_listener(addr).install()?;

    info!("📊 Metrics initialized on port {}", port);
    Ok(())
}
