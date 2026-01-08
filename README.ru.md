# Reorg-Safe Onchain Data Platform

> Индексер блокчейна с корректной обработкой реорганизаций

[English](README.md) | Русский

![Rust](https://img.shields.io/badge/Rust-1.92+-orange?logo=rust)
![License](https://img.shields.io/badge/license-MIT-blue)
![Build](https://img.shields.io/badge/build-passing-brightgreen)

---

## Что это?

Система индексации блокчейн-данных, которая **правильно обрабатывает реорги**.

Большинство индексеров либо игнорируют реорганизации, либо обрабатывают их криво. Этот проект отслеживает каноническую цепочку, обнаруживает форки, находит общего предка (LCA) и гарантирует консистентность данных даже когда цепочка перестраивается.

Построен для Ethereum, спроектирован для расширения на другие сети.

### Ключевые особенности

- **Reorg-safe архитектура** — Обнаружение реоргов, поиск LCA, маркировка orphan-блоков
- **Двухслойная модель данных** — Раздельные слои `head` (реалтайм) и `finalized` (подтверждённые)
- **Идемпотентная обработка** — Безопасные рестарты, переиндексация, восстановление после сбоев
- **Полная наблюдаемость** — Prometheus метрики, Grafana дашборды, структурированные логи
- **Production-паттерны** — Graceful shutdown, health checks, backpressure

---

## Зачем я это сделал

Это pet-проект в рамках моего обучения Backend Data и DevOps разработки в рамках Blockchain.

Этот проект показывает, как строить инфраструктуру индексации, которая справляется с грязной реальностью блокчейн-консенсуса, где "последний блок" может перестать быть последним через 30 секунд.

## Архитектура

### Сервисы

| Сервис | Порт | Вход | Выход | Хранилище |
|--------|------|------|-------|-----------|
| **Ingest** | 9091 | Ethereum RPC | Сырые блоки и логи | ClickHouse: `raw_logs_head` |
| **Decode** | 9092 | `raw_logs_head` | Типизированные события | ClickHouse: `erc20_transfers_head` |
| **Finalizer** | 9094 | Head таблицы | Подтверждённые данные | ClickHouse: `*_finalized` |
| **API** | 4000 | HTTP запросы | JSON ответы | Читает из обеих БД |

### Базы данных

| База | Назначение | Таблицы |
|------|------------|---------|
| **ClickHouse** | Хранение событий (аналитика) | `raw_logs_head`, `erc20_transfers_head`, `*_finalized` |
| **Postgres** | Control plane (транзакции) | `canonical_blocks`, `chain_state`, `reorg_audit` |

### Поток данных

1. **Ingest** получает блоки из RPC, обнаруживает реорги, пишет сырые логи
2. **Decode** читает сырые логи, извлекает ERC-20 Transfer события
3. **Finalizer** переносит подтверждённые блоки (глубина > 64) в finalized таблицы
4. **API** обслуживает запросы, фильтрует orphan через JOIN на canonical

### Как работает обнаружение реоргов

1. Приходит новый блок
2. Сравниваем `parent_hash` с хэшем нашего canonical tip
3. Если не совпадает → идём назад по `parent_hash` пока не найдём общего предка (LCA)
4. Помечаем все блоки после LCA как `orphan`
5. Записываем событие в `reorg_audit`
6. Продолжаем индексацию с новой ветки

Данные в ClickHouse не удаляются сразу — orphan-данные фильтруются через JOIN на canonical блоки. Это делает систему идемпотентной.

---

## Быстрый старт

### Требования

- Docker и Docker Compose
- Rust 1.92+ (если собираете из исходников)
- Ethereum RPC endpoint (Alchemy, Infura или своя нода)

### Запуск через Docker

```bash
# Клонируем
git clone https://github.com/yourusername/onchain-data-platform.git
cd onchain-data-platform

# Настраиваем
cp .env.example .env
# Редактируем .env — добавляем свой RPC_URL

# Поднимаем инфраструктуру
make up

# Останавливаем инфраструктуру
make down

# ОСТОРОЖНО! Удаляем все БД с данными
make nuke

# Применяем миграции
make migrate

# Запускаем все сервисы
cargo run --package ingest
cargo run --package decode
cargo run --package finalizer
cargo run --package api
```

### Проверяем что работает
```bash
# Health check
curl http://localhost:4000/health

# Текущее состояние индексера
curl http://localhost:4000/head

# Последние ERC-20 трансферы
curl http://localhost:4000/transfers?limit=10
```

## Структура проекта
```text
├── crates/
│   └── common/              # Общий код: БД, модели, конфиг
│
├── services/
│   ├── ingest/              # Получает блоки, обнаруживает реорги
│   ├── decode/              # Декодирует логи → типизированные события
│   ├── finalizer/           # Переносит подтверждённые данные
│   └── api/                 # REST API
│
├── db/
│   ├── postgres/            # Схема control plane
│   └── clickhouse/          # Схема data layer
│
├── deploy/
│   └── docker-compose.yml
│
└── dashboards/              # Grafana дашборды
```

## API

### GET /health
Проверка здоровья сервиса.

### GET /head
Возвращает текущее состояние индексера.

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
| Параметр | Тип | Описание |
|----------|-----|----------|
| token | string | Фильтр по адресу токена |
| from_block |	int |	Начальный блок |
| to_block |	int |	Конечный блок |
| limit |	int |	Макс. результатов (по умолчанию: 50, макс: 1000) |
| layer |	string |	head или finalized |

[Полная документация API](docs/openapi.yaml)

## 🛠️ Стек
- Core: Rust (Tokio, Axum, Alloy, SQLx)
- Storage: ClickHouse (Analytics), Postgres (State)
- Ops: Docker Compose, Prometheus, Grafana