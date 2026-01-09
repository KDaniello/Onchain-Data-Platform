use config::{Config, ConfigError};
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub server: ServerSettings,
    pub database: DatabaseSettings,
    pub clickhouse: ClickHouseSettings,
    pub chain: ChainSettings,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerSettings {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseSettings {
    pub url: String,
    pub max_connections: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ClickHouseSettings {
    pub url: String,
    pub user: String,
    pub password: Option<String>,
    pub db: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ChainSettings {
    pub rpc_url: String,
    pub chain_id: u64,
    pub start_block: u64,
    pub reorg_depth: u64,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        dotenv::dotenv().ok();

        let mut builder = Config::builder()
            .set_default("server.host", "0.0.0.0")?
            .set_default("server.port", 4000)?
            .set_default("database.max_connections", 5)?
            .set_default("chain.start_block", 0)?
            .set_default("chain.reorg_depth", 64)?;

        // Manual

        // Database
        let db_url = env::var("DATABASE_URL")
            .map_err(|_| ConfigError::Message("Env var DATABASE_URL is missing".into()))?;
        builder = builder.set_override("database.url", db_url)?;

        // Chain RPC
        let rpc_url = env::var("APP_CHAIN__RPC_URL")
            .map_err(|_| ConfigError::Message("Env var APP_CHAIN__RPC_URL is missing".into()))?;
        builder = builder.set_override("chain.rpc_url", rpc_url)?;

        // Chain ID
        if let Ok(val) = env::var("APP_CHAIN__CHAIN_ID") {
            builder = builder.set_override("chain.chain_id", val)?;
        }

        // Start Block
        if let Ok(val) = env::var("APP_CHAIN__START_BLOCK") {
            builder = builder.set_override("chain.start_block", val)?;
        }

        // ClickHouse
        if let Ok(val) = env::var("APP_CLICKHOUSE__URL") {
            builder = builder.set_override("clickhouse.url", val)?;
        }
        if let Ok(val) = env::var("APP_CLICKHOUSE__USER") {
            builder = builder.set_override("clickhouse.user", val)?;
        }
        if let Ok(val) = env::var("APP_CLICKHOUSE__DB") {
            builder = builder.set_override("clickhouse.db", val)?;
        }
        if let Ok(val) = env::var("APP_CLICKHOUSE__PASSWORD") {
            builder = builder.set_override("clickhouse.password", val)?;
        }

        builder.build()?.try_deserialize()
    }
}
