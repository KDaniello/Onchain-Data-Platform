# Reorg-Safe Onchain Data Platform

> Production-grade blockchain indexer with proper chain reorganization handling

English | [Русский](README.ru.md)

![Rust](https://img.shields.io/badge/Rust-1.92+-orange?logo=rust)
![License](https://img.shields.io/badge/license-MIT-blue)
![Build](https://img.shields.io/badge/build-passing-brightgreen)

---

## What is this?

A blockchain data indexing system that **actually handles reorgs correctly**. 

Most indexers either ignore reorganizations or handle them poorly. This one tracks canonical chain state, detects forks, finds common ancestors, and ensures your data stays consistent even when the chain reorganizes.

Built for Ethereum, designed to extend to other chains.

### Key Features

- **Reorg-safe architecture** — Detects reorganizations, finds LCA, marks orphaned blocks
- **Two-layer data model** — Separate `head` (real-time) and `finalized` (confirmed) data
- **Idempotent processing** — Safe to restart, re-index, or recover from crashes
- **Full observability** — Prometheus metrics, Grafana dashboards, structured logging
- **Production patterns** — Graceful shutdown, health checks, backpressure handling

---

## Why I built this

This is a pet project as part of my training in Backend Data and DevOps development in the Blockchain.

This project demonstrates how to build indexing infrastructure that handles the messy reality of blockchain consensus — where the "latest block" might not be the latest block in 30 seconds.

---

## Architecture

### Services

| Service | Port | Input | Output | Storage |
|---------|------|-------|--------|---------|
| **Ingest** | 9091 | Ethereum RPC | Raw blocks & logs | ClickHouse: `raw_logs_head` |
| **Decode** | 9092 | `raw_logs_head` | Typed events | ClickHouse: `erc20_transfers_head` |
| **Finalizer** | 9094 | Head tables | Confirmed data | ClickHouse: `*_finalized` |
| **API** | 4000 | HTTP requests | JSON responses | Reads from both DBs |

### Databases

| Database | Purpose | Tables |
|----------|---------|--------|
| **ClickHouse** | Event storage (analytical) | `raw_logs_head`, `erc20_transfers_head`, `*_finalized` |
| **Postgres** | Control plane (transactional) | `canonical_blocks`, `chain_state`, `reorg_audit` |

### Data Flow

1. **Ingest** fetches blocks from RPC, detects reorgs, writes raw logs
2. **Decode** reads raw logs, extracts ERC-20 Transfer events
3. **Finalizer** moves confirmed blocks (depth > 64) to finalized tables
4. **API** serves queries, filters orphaned data via canonical JOIN

### How reorg detection works

1. New block arrives
2. Compare `parent_hash` with our canonical tip
3. If mismatch → walk back via `parent_hash` until we find common ancestor (LCA)
4. Mark all blocks after LCA as `orphan`
5. Log to `reorg_audit` table
6. Continue indexing from new chain

Data in ClickHouse is never deleted immediately — orphaned data is filtered out via JOIN on canonical blocks. This keeps the system idempotent.

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Rust 1.92+ (if building from source)
- Ethereum RPC endpoint (Alchemy, Infura, or your own node)

### Run with Docker

```bash
# Clone
git clone https://github.com/yourusername/onchain-data-platform.git
cd onchain-data-platform

# Configure
cp .env.example .env
# Edit .env — add your RPC_URL

# Start infrastructure
make up

# Stop infrastructure
make down

# DANGER! Delete all data
make nuke

# Apply migrations
make migrate

# Run Services
cargo run --package ingest
cargo run --package decode
cargo run --package finalizer
cargo run --package api
```

### Verify it works

```bash
# Health check
curl http://localhost:4000/health

# Current indexer head
curl http://localhost:4000/head

# Recent ERC-20 transfers
curl http://localhost:4000/transfers?limit=10
```

## Project Structure

```text
├── crates/
│   └── common/              # Shared code: DB, models, config
│
├── services/
│   ├── ingest/              # Fetches blocks, detects reorgs
│   ├── decode/              # Decodes raw logs → typed events
│   ├── finalizer/           # Moves confirmed data to finalized layer
│   └── api/                 # REST API
│
├── db/
│   ├── postgres/            # Control plane schema
│   └── clickhouse/          # Data layer schema
│
├── deploy/
│   └── docker-compose.yml
│
└── dashboards/              # Grafana dashboards
```

## API Reference
### GET /health
Health check endpoint.

### GET /head
Returns current indexer state.
```JSON
{
  "chain_id": 1,
  "head_number": 19543210,
  "head_hash": "0xabc...",
  "finalized_number": 19543146,
  "lag_blocks": 2,
  "updated_at": "2024-01-15T10:30:00Z"
}
```

### GET /transfers
Query ERC-20 transfers.

| Parameter | Type | Description |
|-----------|------|-------------|
| token | string | Filter by token address
| from_block | int | Start block
| to_block | int | End block
| limit | int | Max results (default: 50, max: 1000)
| layer | string | head or finalized

[Full API documentation](docs/openapi.yaml)

## 🛠️ Tech Stack
- Core: Rust (Tokio, Axum, Alloy, SQLx)
- Storage: ClickHouse (Analytics), Postgres (State)
- Ops: Docker Compose, Prometheus, Grafana