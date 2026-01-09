use crate::settings::{ClickHouseSettings, DatabaseSettings};
use anyhow::{Context, Result};
use clickhouse::Client as ClickHouseClient;
use sqlx::postgres::{PgPool, PgPoolOptions};

pub async fn connect_pg(settings: &DatabaseSettings) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(settings.max_connections)
        .connect(&settings.url)
        .await
        .context("Failed to connect to Postgres")
}

pub fn connect_ch(settings: &ClickHouseSettings) -> ClickHouseClient {
    let mut client = ClickHouseClient::default()
        .with_url(&settings.url)
        .with_user(&settings.user)
        .with_database(&settings.db);

    if let Some(password) = &settings.password {
        client = client.with_password(password);
    }

    client
}
