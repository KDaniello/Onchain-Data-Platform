CREATE DATABASE IF NOT EXISTS onchain_data;

-- Основная таблица для логов (HEAD слой)
-- Используем ReplacingMergeTree, чтобы теоретически можно было схлопнуть дубли, 
-- но полагаемся мы на идемпотентность в коде.
CREATE TABLE IF NOT EXISTS onchain_data.raw_logs_head
(
    chain_id UInt64,
    block_number UInt64,
    block_hash String,     -- String проще для hex в CH
    tx_hash String,
    log_index UInt32,
    address String,
    topic0 String,         -- Event signature
    topic1 String,
    topic2 String,
    topic3 String,
    data String,           -- Raw bytes (hex)
    block_timestamp DateTime,
    inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree()
ORDER BY (chain_id, block_number, log_index, address);
-- ORDER BY выбираем так, чтобы быстро искать логи конкретного блока или контракта