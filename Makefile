.PHONY: up down logs db-shell

# Поднять инфраструктуру
up:
	docker-compose -f deploy/docker-compose.yml up -d

# Уронить инфраструктуру
down:
	docker-compose -f deploy/docker-compose.yml down

# Логи
logs:
	docker-compose -f deploy/docker-compose.yml logs -f

# Зайти в SQL консоль Postgres
pg-shell:
	docker exec -it odp-postgres psql -U admin -d onchain_data

# Зайти в клиент Clickhouse
ch-shell:
	docker exec -it odp-clickhouse clickhouse-client