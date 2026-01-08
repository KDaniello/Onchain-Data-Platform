use tokio::signal;
use tokio::sync::broadcast;
use tracing::info;

/// Create channel to signal shutdown
/// Return Sender. Services will create Receiver with .subscribe()
pub fn shutdown_signal() -> broadcast::Sender<()> {
    let (tx, _) = broadcast::channel(1);
    let tx_clone = tx.clone();

    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                info!("🛑 Received Ctrl+C. Initiating shutdown...");
            },
            Err(err) => {
                info!("Unable to listen for shutdown signal: {}", err);
            },
        }

        let _ = tx_clone.send(());
    });

    tx
}