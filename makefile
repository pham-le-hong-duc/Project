# Makefile for OKX Data Pipeline Project
# Usage: make <target>

# Workflow order:
# 0. build           -> Build Docker images
# 1. infra-up        -> Start infrastructure (redpanda, timescaledb, minio, airflow)
# 2. ingest-up       -> Start real-time data ingestion
# 3. dag01           -> Trigger DAG 01 (ingest_historical_data)
# 4. aggregate-up    -> Start real-time data aggregation
# 5. dag02           -> Trigger DAG 02 (aggregate_historical_data)

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  help                    - Show this help message"
	@echo ""
	@echo "=== Workflow (Sequential Order) ==="
	@echo "  0. build               - Build Docker images"
	@echo "  1. infra-up            - Start infrastructure services"
	@echo "  2. ingest-up           - Start real-time data ingestion"
	@echo "  3. dag01               - Trigger DAG 01 (ingest_historical_data)"
	@echo "  4. aggregate-up        - Start real-time data aggregation"
	@echo "  5. dag02               - Trigger DAG 02 (aggregate_historical_data)"
	@echo ""
	@echo "  start-all              - Run entire workflow automatically (with build)"
	@echo ""
	@echo "=== Infrastructure ==="
	@echo "  infra-up               - Start infrastructure services"
	@echo "  infra-down             - Stop infrastructure services"
	@echo "  infra-logs             - View infrastructure logs"
	@echo "  infra-status           - Check infrastructure health"
	@echo ""
	@echo "=== Real-time Data Ingestion ==="
	@echo "  ingest-up              - Start real-time data ingestion"
	@echo "  ingest-down            - Stop real-time data ingestion"
	@echo "  ingest-logs            - View ingestion logs"
	@echo ""
	@echo "=== Real-time Data Aggregation ==="
	@echo "  aggregate-up           - Start real-time data aggregation"
	@echo "  aggregate-down         - Stop real-time data aggregation"
	@echo "  aggregate-logs         - View aggregation logs"
	@echo ""
	@echo "=== Airflow DAGs ==="
	@echo "  dag01                  - Trigger DAG 01 (ingest_historical_data)"
	@echo "  dag02                  - Trigger DAG 02 (aggregate_historical_data)"
	@echo "  dag-list               - List all DAGs"
	@echo "  dag-status             - Show DAG run status"
	@echo ""
	@echo "=== Combined Operations ==="
	@echo "  up                     - Start all services (no DAGs)"
	@echo "  down                   - Stop all services"
	@echo "  restart                - Restart all services"
	@echo "  logs                   - View all logs"
	@echo "  status                 - Show status of all services"
	@echo ""
	@echo "=== Build & Clean ==="
	@echo "  build                  - Build Docker images"
	@echo "  clean                  - Remove containers and volumes"
	@echo "  clean-all              - Remove everything including images"

# Infrastructure targets
.PHONY: infra-up infra-down infra-logs infra-status
infra-up:
	@echo "========================================================="
	@echo "STEP 1: Starting infrastructure services..."
	@echo "========================================================="
	docker-compose -f docker/docker-compose.infrastructure.yml --env-file .env up -d --wait
	@echo "[OK] All infrastructure services started!"

infra-down:
	@echo "Stopping infrastructure services..."
	docker-compose -f docker/docker-compose.infrastructure.yml --env-file .env down

infra-logs:
	docker-compose -f docker/docker-compose.infrastructure.yml logs -f

infra-status:
	@echo "Checking infrastructure health..."
	@docker-compose -f docker/docker-compose.infrastructure.yml ps
	@echo ""
	@echo "Health checks:"
	@docker exec redpanda rpk cluster health 2>/dev/null && echo "[OK] Redpanda: OK" || echo "[ERROR] Redpanda: NOT READY"
	@docker exec timescaledb pg_isready -U okx_user -d okx 2>/dev/null && echo "[OK] TimescaleDB: OK" || echo "[ERROR] TimescaleDB: NOT READY"
	@powershell -Command "& { docker exec minio curl -sf http://localhost:9000/minio/health/live 2>&1 | Out-Null; if (\$$LASTEXITCODE -eq 0) { Write-Host '[OK] MinIO: OK' } else { Write-Host '[ERROR] MinIO: NOT READY' } }"

# Real-time ingestion targets
.PHONY: ingest-up ingest-down ingest-logs
ingest-up:
	@echo "========================================================="
	@echo "STEP 2: Starting real-time data ingestion..."
	@echo "========================================================="
	docker-compose -f docker/docker-compose.ingest_realtime_data.yml --env-file .env up -d
	@echo "[OK] All real-time data ingestion services started!"

ingest-down:
	@echo "Stopping real-time data ingestion..."
	docker-compose -f docker/docker-compose.ingest_realtime_data.yml --env-file .env down

ingest-logs:
	docker-compose -f docker/docker-compose.ingest_realtime_data.yml logs -f

# Real-time aggregation targets
.PHONY: aggregate-up aggregate-down aggregate-logs
aggregate-up:
	@echo "========================================================="
	@echo "STEP 4: Starting real-time data aggregation..."
	@echo "========================================================="
	docker-compose -f docker/docker-compose.aggregate_realtime_data.yml --env-file .env up -d
	@echo "[OK] All real-time data aggregation services started!"

aggregate-down:
	@echo "Stopping real-time data aggregation..."
	docker-compose -f docker/docker-compose.aggregate_realtime_data.yml --env-file .env down

aggregate-logs:
	docker-compose -f docker/docker-compose.aggregate_realtime_data.yml logs -f

# Airflow DAG targets
dag01:
	@echo "========================================================="
	@echo "STEP 3: Running DAG 01: Historical data ingestion..."
	@echo "========================================================="
	@powershell -Command "& { \$$timestamp = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss'); docker exec airflow-webserver airflow dags trigger -e \$$timestamp 01_ingest_historical_data 2>&1 | Out-Null; do { Start-Sleep -Seconds 5; \$$status = (docker exec airflow-webserver airflow dags state 01_ingest_historical_data \$$timestamp).Trim(); } while (\$$status -match 'running|queued|null|restarting'); if (\$$status -ne 'success') { Write-Host ('[ERROR] DAG Failed: ' + \$$status) -ForegroundColor Red; exit 1 } }"
	@echo "[OK] DAG 01 Finished and Recorded!"

dag02:
	@echo "========================================================="
	@echo "STEP 5: Running DAG 02: Historical data aggregation..."
	@echo "========================================================="
	@powershell -Command "& { \$$timestamp = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss'); docker exec airflow-webserver airflow dags trigger -e \$$timestamp 02_aggregate_historical_data 2>&1 | Out-Null; do { Start-Sleep -Seconds 5; \$$status = (docker exec airflow-webserver airflow dags state 02_aggregate_historical_data \$$timestamp).Trim(); } while (\$$status -match 'running|queued|null|restarting'); if (\$$status -ne 'success') { Write-Host ('[ERROR] DAG Failed: ' + \$$status) -ForegroundColor Red; exit 1 } }"
	@echo "[OK] DAG 02 Finished and Recorded!"

dag-list:
	@echo "Available DAGs:"
	@docker exec airflow-webserver airflow dags list

dag-status:
	@echo "DAG run status:"
	@docker exec airflow-webserver airflow dags list-runs -d 01_ingest_historical_data --state running --state success --state failed | head -n 5
	@docker exec airflow-webserver airflow dags list-runs -d 02_aggregate_historical_data --state running --state success --state failed | head -n 5

# Combined operations
.PHONY: up down restart logs status start-all
up: infra-up ingest-up aggregate-up
	@echo "All services started successfully (without DAGs)!"

down: aggregate-down ingest-down infra-down
	@echo "All services stopped successfully!"

restart: down up
	@echo "All services restarted successfully!"

logs:
	@echo "Viewing all logs (press Ctrl+C to exit)..."
	docker-compose -f docker/docker-compose.infrastructure.yml -f docker/docker-compose.ingest_realtime_data.yml -f docker/docker-compose.aggregate_realtime_data.yml logs -f

status:
	@echo "=== Infrastructure Services ==="
	@docker-compose -f docker/docker-compose.infrastructure.yml ps
	@echo ""
	@echo "=== Ingestion Services ==="
	@docker-compose -f docker/docker-compose.ingest_realtime_data.yml ps
	@echo ""
	@echo "=== Aggregation Services ==="
	@docker-compose -f docker/docker-compose.aggregate_realtime_data.yml ps

start-all:
	@echo ""
	@echo ""
	@$(MAKE) --no-print-directory build
	@echo ""
	@echo ""
	@$(MAKE) --no-print-directory infra-up
	@echo ""
	@echo ""
	@$(MAKE) --no-print-directory ingest-up
	@echo ""
	@echo ""
	@$(MAKE) --no-print-directory dag01
	@echo ""
	@echo ""
	@$(MAKE) --no-print-directory aggregate-up
	@echo ""
	@echo ""
	@$(MAKE) --no-print-directory dag02

# Build targets
.PHONY: build check-docker
check-docker:
	@echo "Checking Docker availability..."
	@powershell -Command "& { docker info 2>&1 | Out-Null; if (\$$LASTEXITCODE -ne 0) { Write-Host '[ERROR] Docker is not running!'; Write-Host ''; Write-Host 'Please start Docker Desktop and try again.'; exit 1 } else { Write-Host '[OK] Docker is running' } }"

build:
	@echo "========================================================="
	@echo "STEP 0: Building Docker images..."
	@echo "========================================================="
	@echo "Building custom application image (okx-ingestion)..."
	@echo "Note: Docker will cache layers for faster rebuilds"
	docker-compose -f docker/docker-compose.infrastructure.yml --env-file .env build
	@echo "[OK] Docker images built successfully!"

# Clean targets
.PHONY: clean clean-all
clean:
	@echo "Removing containers and volumes..."
	docker-compose -f docker/docker-compose.infrastructure.yml down -v
	docker-compose -f docker/docker-compose.ingest_realtime_data.yml down -v
	docker-compose -f docker/docker-compose.aggregate_realtime_data.yml down -v

clean-all: clean
	@echo "Removing Docker images..."
	docker rmi okx-ingestion:latest || true
	@echo "Cleanup completed!"

