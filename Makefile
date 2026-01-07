.PHONY: up down nuke logs pg-shell ch-shell migrate

COMPOSE_FILE=deploy/docker-compose.yml
PG_CONTAINER=odp-postgres
CH_CONTAINER=odp-clickhouse
PG_DB=onchain_data
PG_USER=admin

PG_MIGRATION_FILE=db/postgres/migrations/000_init_schema.sql
CH_MIGRATION_FILE=db/clickhouse/migrations/000_init_tables.sql

# Main Commands

# Up
up:
	@echo "🚀 Starting infrastructure..."
	docker-compose -f $(COMPOSE_FILE) up -d
	@$(MAKE) wait-for-db
	@$(MAKE) migrate
	@echo "✅ System is READY! Run 'cargo run' to start services."

# 2. Stop
down:
	docker-compose -f $(COMPOSE_FILE) down

# 3. DELETE
nuke:
	@echo "💥 Destroying everything (including data)..."
	docker-compose -f $(COMPOSE_FILE) down -v
	@echo "🧹 Cleaned."


# Helper Commands
wait-for-db:
	@echo "⏳ Waiting for Postgres..."
	@until docker exec $(PG_CONTAINER) pg_isready -U $(PG_USER) > /dev/null 2>&1; do \
		echo "   ...postgres loading"; \
		sleep 2; \
	done
	@echo "✅ Postgres is up."

	@echo "⏳ Waiting for ClickHouse..."
	@until docker exec $(CH_CONTAINER) clickhouse-client --query "SELECT 1" > /dev/null 2>&1; do \
		echo "   ...clickhouse loading"; \
		sleep 2; \
	done
	@echo "✅ ClickHouse is up."

migrate:
	@echo "📦 Migrating Postgres..."
	cat $(PG_MIGRATION_FILE) | docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB)
	
	@echo "📦 Migrating ClickHouse..."
	# Флаг --echo выведет выполняемые запросы, чтобы видеть, где упало
	cat $(CH_MIGRATION_FILE) | docker exec -i $(CH_CONTAINER) clickhouse-client --multiquery --echo

logs:
	docker-compose -f $(COMPOSE_FILE) logs -f

pg-shell:
	docker exec -it $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB)

ch-shell:
	docker exec -it $(CH_CONTAINER) clickhouse-client