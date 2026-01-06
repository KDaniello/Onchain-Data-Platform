CREATE DATABASE IF NOT EXISTS onchain_data;

CREATE TABLE IF NOT EXISTS onchain_data.raw_logs_head
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
    block_timestamp DateTime,
    inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree()
ORDER BY (chain_id, block_number, log_index, address);