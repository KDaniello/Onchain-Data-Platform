
### 4.3 Architecture Decision Records (ADR)

Создай `docs/adr/001-reorg-handling.md`.

```markdown
# ADR 001: Reorg Handling Strategy

## Context
Blockchain reorganizations invalidate previously indexed data. Deleting data from OLAP databases (ClickHouse) is expensive and slow (`ALTER DELETE`).

## Decision
We implement a **Logical Deletion** strategy:
1. Block status (`canonical` / `orphan`) is tracked in Postgres.
2. Ingestion marks reorged blocks as `orphan` in Postgres immediately.
3. API and Decoder queries join against the Postgres canonical list.
4. Data remains in ClickHouse but is filtered out at read time.

## Consequences
- **Positive**: Instant reorg handling; no expensive mutations in ClickHouse.
- **Negative**: "Dead" data accumulates in ClickHouse (requires background TTL cleanup later).