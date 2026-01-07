use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub server: ServerSettings,
    pub database: DatabaseSettings,
    pub clickhouse: ClickHouseSettings,
    pub chain: ChainSettings
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerSettings {
    pub host: String,
    pub port: u16
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseSettings {
    pub url: String,
    pub max_connections: u32
}

#[derive(Debug, Deserialize, Clone)]
pub struct ClickHouseSettings {
    pub url: String,
    pub user: String,
    pub password: Option<String>,
    pub db: String
}

#[derive(Debug, Deserialize, Clone)]
pub struct ChainSettings {
    pub rpc_url: String,
    pub chain_id: u64,
    pub start_block: u64,
    pub reorg_depth: u64
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let run_mode = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());

        let s = Config::builder()
            .set_default("server.host", "0.0.0.0")?
            .set_default("server.port", 4000)?
            .set_default("chain.start_block", 0)?
            .set_default("chain.reorg_depth", 64)?
            .set_default("database.max_connections", 5)?
            .add_source(Environment::with_prefix("APP").separator("__"))
            .build()?;

        s.try_deserialize()
    }
}