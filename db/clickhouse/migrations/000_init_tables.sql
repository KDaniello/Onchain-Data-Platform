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
    block_timestamp DateTime
)
ENGINE = ReplacingMergeTree()
ORDER BY (chain_id, block_number, block_hash, log_index);


-- Transfers
-- Use MATERIALIZED column for uint256 without rust-driver (inside Clickhouse)
DROP TABLE IF EXISTS onchain_data.erc20_transfers;
CREATE TABLE onchain_data.erc20_transfers
(
    chain_id UInt64,
    block_number UInt64,
    block_hash String,
    tx_hash String,
    log_index UInt32,
    block_timestamp DateTime,
    token_address String,
    from String,
    to String,
    value String,
    value_approx Float64
)
ENGINE = ReplacingMergeTree()
ORDER BY (chain_id, token_address, block_number, log_index);