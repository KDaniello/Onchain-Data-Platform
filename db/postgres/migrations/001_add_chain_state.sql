-- Global state
CREATE TABLE chain_state (
    chain_id BIGINT PRIMARY KEY,
    head_number BIGINT NOT NULL DEFAULT 0,
    head_hash CHAR(66) NOT NULL DEFAULT '',
    finalized_number BIGINT NOT NULL DEFAULT 0,
    finalized_hash CHAR(66),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Reorg Audit table
CREATE TABLE reorg_audit (
    id SERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    old_head_number BIGINT NOT NULL,
    old_head_hash CHAR(66) NOT NULL,
    new_head_number BIGINT NOT NULL,
    new_head_hash CHAR(66) NOT NULL,
    lca_number BIGINT NOT NULL,        -- Lowest Common Ancestor
    lca_hash CHAR(66) NOT NULL,
    depth INT NOT NULL,                 -- Reorg deoth
    blocks_orphaned INT NOT NULL,       -- Count of blocks marked orphan
    reprocessed BOOLEAN DEFAULT FALSE,  -- Flag: if rewrite blocks after reorg
    details JSONB                       -- Debug
);

CREATE INDEX idx_reorg_audit_chain ON reorg_audit(chain_id, detected_at DESC);

-- Crucial index for fast search of orphan-blocks
CREATE INDEX IF NOT EXISTS idx_blocks_orphan ON canonical_blocks(chain_id, status) 
WHERE status = 'orphan';