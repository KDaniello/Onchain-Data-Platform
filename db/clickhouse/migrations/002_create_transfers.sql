CREATE TABLE IF NOT EXISTS onchain_data.erc20_transfers
(
    chain_id UInt64,
    block_number UInt64,
    block_hash String,
    tx_hash String,
    log_index UInt32,
    block_timestamp DateTime,
    
    -- Dedoded raws
    token_address String,
    from String,
    to String,
    value String,
    value_numeric Float64 -- for metrics
)
ENGINE = ReplacingMergeTree()
ORDER BY (chain_id, token_address, block_number, log_index);