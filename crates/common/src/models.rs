use sqlx::{FromRow, Type};
use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::{Deserialize, Serialize};

/// Block's status on chain
/// Canonical - part of canonical chain.
/// Orphan - reorg chain.
/// Finalized - Block in final DB.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum BlockStatus {
    #[default]
    Canonical,
    Orphan,
    Finalized
}

// Blockstatus -> String in Postgres
impl Type<sqlx::Postgres> for BlockStatus {
    fn type_info() -> <sqlx::Postgres as sqlx::Database>::TypeInfo {
        <String as Type<sqlx::Postgres>>::type_info()
    }
}

// Status -> String
impl sqlx::Encode<'_, sqlx::Postgres> for BlockStatus {
    fn encode_by_ref(
            &self,
            buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'_>,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let s = match self {
            BlockStatus::Canonical => "canonical",
            BlockStatus::Orphan => "orphan",
            BlockStatus::Finalized => "finalized"
        };
        <String as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&s.to_string(), buf)
    }
}

// String -> Status
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for BlockStatus {
    fn decode(value: <sqlx::Postgres as sqlx::Database>::ValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s: String = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        match s.as_str() {
            "canonacal" => Ok(BlockStatus::Canonical),
            "orphan" => Ok(BlockStatus::Orphan),
            "finalized" => Ok(BlockStatus::Finalized),
            _ => Err(format!("Unknown block status: {}", s).into()),
        }
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct CanonicalBlock {
    pub chain_id: i64,
    pub number: i64,
    pub hash: String,
    pub parent_hash: String,
    pub block_timestamp: DateTime<Utc>,
    pub status: BlockStatus,
    pub inserted_at: Option<DateTime<Utc>>
}

#[derive(Debug, Clone, FromRow)]
pub struct Chainstate {
    pub chain_id: i64,
    pub head_number: i64,
    pub head_hash: String,
    pub finalized_number: i64,
    pub updated_at: Option<DateTime<Utc>>
}

/// Model for ClickHouse
/// Derives: Row (to insert), Serialize (to send)
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct RawLog {
    pub chain_id: u64,
    pub block_number: u64,
    pub block_hash: String,
    pub tx_hash: String,
    pub log_index: u32,
    pub address: String,
    pub topic0: String,
    pub topic1: String,
    pub topic2: String,
    pub topic3: String,
    pub data: String,
    pub block_timestamp: u32
}

/// Decode model
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct Erc20Transfer {
    pub chain_id: u64,
    pub block_number: u64,
    pub block_hash: String,
    pub tx_hash: String,
    pub log_index: u32,
    pub block_timestamp: u32,
    // Decoded
    pub token_address: String,
    pub from: String,
    pub to: String,
    pub value: String,
    pub value_numeric: f64
}