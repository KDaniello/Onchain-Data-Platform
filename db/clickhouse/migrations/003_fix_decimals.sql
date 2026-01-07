DROP TABLE IF EXISTS onchain_data.erc20_transfers;

CREATE TABLE IF NOT EXISTS onchain_data.erc20_transfers
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
    
    value String,           -- Raw string (для совместимости)
    value_exact Decimal256(0), -- Точное число для агрегаций (SUM, AVG)
    value_numeric Float64   -- Оставляем для быстрых графиков
)
ENGINE = ReplacingMergeTree()
ORDER BY (chain_id, token_address, block_number, log_index);