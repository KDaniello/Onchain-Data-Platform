-- 1. Удаляем старую таблицу
DROP TABLE IF EXISTS canonical_blocks;

-- 2. Новую таблица, где PK - это хэш, а не номер
CREATE TABLE canonical_blocks (
    chain_id BIGINT NOT NULL,
    number BIGINT NOT NULL,
    hash CHAR(66) NOT NULL,          -- Уникальный идентификатор блока
    parent_hash CHAR(66) NOT NULL,   -- Ссылка на предка
    block_timestamp TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'canonical', -- 'canonical', 'orphan'
    inserted_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (chain_id, hash)
);

-- Индекс для быстрого поиска канонического блока по номеру
-- Уникальный индекс с условием: на одной высоте может быть только один CANONICAL блок
CREATE UNIQUE INDEX idx_canonical_height 
ON canonical_blocks(chain_id, number) 
WHERE status = 'canonical';

-- Индекс для поиска детей (чтобы быстро строить цепочку)
CREATE INDEX idx_parent_hash ON canonical_blocks(parent_hash);