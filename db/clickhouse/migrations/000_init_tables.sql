CREATE DATABASE IF NOT EXISTS onchain_data;

-- Rawlogs
-- ReplacingMergeTree (Deduplication by key)
DROP TABLE IF EXISTS onchain_data.raw_logs_head;
CREATE TABLE onchain_data.raw_logs_head
(
    chain_id UInt64,
    block_number UInt64,
    block_hash String,
    tx_hash String,
    log_index UInt32,
    address String,
    topic0 String,
    topic1 String,
    topic2 String,
    topic3 String,
    data String,
    block_timestamp UInt32,
    inserted_at UInt64
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(toDateTime(block_timestamp))
ORDER BY (chain_id, block_hash, tx_hash, log_index);

-- Transfers
-- Use MATERIALIZED column for uint256 without rust-driver (inside Clickhouse)
DROP TABLE IF EXISTS onchain_data.erc20_transfers_head;
CREATE TABLE onchain_data.erc20_transfers_head
(
    chain_id UInt64,
    block_number UInt64,
    block_hash String,
    tx_hash String,
    log_index UInt32,
    block_timestamp UInt32,
    token_address String,
    from_address String,
    to_address String,
    value String,
    value_approx Float64,
    inserted_at UInt64
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(toDateTime(block_timestamp))
ORDER BY (chain_id, block_hash, tx_hash, log_index);

-- Finalized Logs (Append-only)
DROP TABLE IF EXISTS onchain_data.raw_logs_finalized;
CREATE TABLE onchain_data.raw_logs_finalized
(
    chain_id UInt64,
    block_number UInt64,
    block_hash String,
    tx_hash String,
    log_index UInt32,
    address String,
    topic0 String,
    topic1 String,
    topic2 String,
    topic3 String,
    data String,
    block_timestamp UInt32,
    inserted_at UInt64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(block_timestamp))
ORDER BY (chain_id, block_number, log_index);

-- 4. Transfers Finalized
DROP TABLE IF EXISTS onchain_data.erc20_transfers_finalized;
CREATE TABLE onchain_data.erc20_transfers_finalized
(
    chain_id UInt64,
    block_number UInt64,
    block_hash String,
    tx_hash String,
    log_index UInt32,
    block_timestamp UInt32,
    token_address String,
    from_address String,
    to_address String,
    value String,
    value_approx Float64,
    inserted_at UInt64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(block_timestamp))
ORDER BY (chain_id, block_number, log_index);

DROP TABLE IF EXISTS onchain_data.erc20_transfers;