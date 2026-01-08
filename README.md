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

I kept seeing blockchain indexers that break silently when reorgs happen. Data becomes inconsistent, balances don't match, events get duplicated or lost.

This project demonstrates how to build indexing infrastructure that handles the messy reality of blockchain consensus — where the "latest block" might not be the latest block in 30 seconds.

---

## Architecture

```mermaid
graph TD
    %% Actors
    User([User / Client])
    RPC[Blockchain RPC]

    %% Subgraph: Infrastructure
    subgraph "Infrastructure"
        direction TB
        PG[(Postgres\nControl Plane)]
        CH[(ClickHouse\nData Lake)]
    end

    %% Subgraph: Microservices
    subgraph "Rust Microservices"
        Ingest[Ingest Service]
        Decode[Decode Service]
        Finalizer[Finalizer Service]
        API[API Service]
    end

    %% Flows - Ingestion
    RPC ==>|Blocks & Logs| Ingest
    Ingest -->|1. Write Canonical Header| PG
    Ingest -->|2. Write Raw Logs| CH
    Ingest -.->|Detect Reorgs| PG

    %% Flows - Decoding
    Decode -->|3. Read Raw Logs| CH
    Decode -->|4. Check Validity| PG
    Decode -->|5. Write Transfers| CH

    %% Flows - Finalization
    Finalizer -->|6. Check Depth| PG
    Finalizer -->|7. Move to Finalized| CH

    %% Flows - API
    User ==>|HTTP Request| API
    API -->|8. Get Canonical List| PG
    API -->|9. Fetch Filtered Data| CH
    API ==>|JSON Response| User

    %% Styling
    classDef storage fill:#e1f5fe,stroke:#01579b,stroke-width:2px;
    classDef service fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px;
    class PG,CH storage;
    class Ingest,Decode,Finalizer,API service;
```