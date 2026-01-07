CREATE DATABASE IF NOT EXISTS onchain_data;

-- Rawlogs
-- ReplacingMergeTree (Deduplication by key)
CREATE TABLE IF NOT EXISTS onchain_data.raw_logs_head
(
    chain_id UInt64,
    block_number UInt64,
    block_hash String,
    tx_hash String,
    log_index UInt32,
    
    address String,        -- lowercase
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
CREATE TABLE IF NOT EXISTS onchain_data.erc20_transfers
(
    chain_id UInt64,
    block_number UInt64,
    block_hash String,
    tx_hash String,
    log_index UInt32,
    block_timestamp DateTime,
    
    token_address String,  -- Lowercase
    from String,           -- Lowercase
    to String,             -- Lowercase
    
    value String,          -- Uint256 in String type
    value_numeric Float64,  -- for visualization in Dashboards
)
ENGINE = ReplacingMergeTree()
ORDER BY (chain_id, token_address, block_number, log_index);