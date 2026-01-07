DROP TABLE IF EXISTS canonical_blocks;
DROP TABLE IF EXISTS decoder_state;

-- Table blocks
CREATE TABLE canonical_blocks (
    chain_id BIGINT NOT NULL,
    number BIGINT NOT NULL,
    hash CHAR(66) NOT NULL,
    parent_hash CHAR(66) NOT NULL,
    block_timestamp TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'canonical',
    inserted_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (chain_id, hash)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_canonical_height 
ON canonical_blocks (chain_id, number) 
WHERE status = 'canonical';

CREATE INDEX IF NOT EXISTS idx_blocks_chain_number ON canonical_blocks(chain_id, number);

-- Table cursors for decoder
CREATE TABLE decoder_state (
    id VARCHAR(50) PRIMARY KEY, -- for ex. 'erc20_worker'
    last_processed_block BIGINT NOT NULL DEFAULT 0,
    last_processed_hash CHAR(66),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO decoder_state (id, last_processed_block) 
VALUES ('finalizer_worker', 0) 
ON CONFLICT (id) DO NOTHING;