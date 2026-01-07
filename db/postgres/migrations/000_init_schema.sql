CREATE TABLE IF NOT EXISTS canonical_blocks (
    chain_id BIGINT NOT NULL,
    number BIGINT NOT NULL,
    hash CHAR(66) NOT NULL,          -- 0x...
    parent_hash CHAR(66) NOT NULL,
    block_timestamp TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'canonical', -- 'canonical', 'orphan'
    inserted_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (chain_id, hash)
);

-- Unique index: only one uniq CANONICAL block
CREATE UNIQUE INDEX IF NOT EXISTS idx_canonical_height 
ON canonical_blocks(chain_id, number) 
WHERE status = 'canonical';

-- index for fast search by index (for API/decoder)
CREATE INDEX IF NOT EXISTS idx_blocks_number ON canonical_blocks(number);