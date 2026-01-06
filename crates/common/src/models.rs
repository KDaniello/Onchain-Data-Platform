use sqlx::{FromRow, Type};
use chrono::{DateTime, Utc};

/// Статус блока в системе.
/// Canonical - часть основной цепочки.
/// Orphan - отброшенная ветка (реорг).
/// Finalized - блок, который мы помещаем в итоговую БД.
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

// Status -> string
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

// string -> Status
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