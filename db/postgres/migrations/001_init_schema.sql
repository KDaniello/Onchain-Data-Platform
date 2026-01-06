-- Таблица состояния цепочки (какой блок последний обработан)
CREATE TABLE chain_state (
    chain_id BIGINT PRIMARY KEY,
    head_number BIGINT NOT NULL,
    head_hash CHAR(66) NOT NULL, -- 0x... (64 hex + 2 prefix)
    finalized_number BIGINT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- "Каноническая" цепочка блоков.
-- Уникальность по (chain_id, number) гарантирует, что у нас только один "истинный" блок на каждой высоте.
CREATE TABLE canonical_blocks (
    chain_id BIGINT NOT NULL,
    number BIGINT NOT NULL,
    hash CHAR(66) NOT NULL,
    parent_hash CHAR(66) NOT NULL,
    block_timestamp TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'canonical', -- 'canonical', 'orphan', 'finalized'
    inserted_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (chain_id, number)
);

-- Индекс для быстрого поиска по хэшу (нужен для детекта реоргов)
CREATE INDEX idx_canonical_hash ON canonical_blocks(chain_id, hash);

-- Аудит реоргов (чтобы мы видели на графиках, когда и что откатилось)
CREATE TABLE reorg_audit (
    id SERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    reorg_depth INT NOT NULL,
    old_tip_number BIGINT NOT NULL,
    old_tip_hash CHAR(66) NOT NULL,
    new_tip_hash CHAR(66) NOT NULL
);