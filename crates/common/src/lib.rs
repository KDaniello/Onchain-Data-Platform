pub mod models;
pub mod settings;
pub mod db;
pub mod shutdown;

// Переэкспорт для удобства (common::CanonicalBlock)
pub use models::*;