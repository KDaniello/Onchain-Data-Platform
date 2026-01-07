.PHONY: up down nuke logs pg-shell ch-shell migrate

COMPOSE_FILE=deploy/docker-compose.yml
PG_CONTAINER=odp-postgres
CH_CONTAINER=odp-clickhouse
PG_DB=onchain_data
PG_USER=admin

# Main Commands

# Up
up:
	@echo "🚀 Starting infrastructure..."
	docker-compose -f $(COMPOSE_FILE) up -d
	@echo "⏳ Waiting for databases to initialize (10s)..."
	@sleep 10
	@$(MAKE) migrate
	@echo "✅ System is up and ready! Run 'cargo run' to start services."

# 2. Stop
down:
	docker-compose -f $(COMPOSE_FILE) down

# 3. DELETE
nuke:
	@echo "💥 Destroying everything (including data)..."
	docker-compose -f $(COMPOSE_FILE) down -v
	@echo "🧹 Cleaned."


# Helper Commands

# Migrations
migrate:
	@echo "📦 Migrating Postgres..."
	cat db/postgres/migrations/000_init_schema.sql | docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB)
	@echo "📦 Migrating ClickHouse..."
	cat db/clickhouse/migrations/000_init_tables.sql | docker exec -i $(CH_CONTAINER) clickhouse-client --multiquery

# Логи
logs:
	docker-compose -f $(COMPOSE_FILE) logs -f

# Консоль Postgres
pg-shell:
	docker exec -it $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB)

# Клиент ClickHouse
ch-shell:
	docker exec -it $(CH_CONTAINER) clickhouse-client