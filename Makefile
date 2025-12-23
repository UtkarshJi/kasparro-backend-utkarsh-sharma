.PHONY: up down test logs build clean help

# Default target
.DEFAULT_GOAL := help

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
NC := \033[0m

## help: Show this help message
help:
	@echo "Kasparro Backend - Makefile Commands"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^## ' Makefile | sed 's/## /  /'

## up: Start all services (database + API + ETL)
up:
	@echo "Starting Kasparro services..."
	docker-compose up -d --build
	@echo "Services started! API available at http://localhost:8000"
	@echo "Swagger docs at http://localhost:8000/docs"

## down: Stop all services
down:
	@echo "Stopping Kasparro services..."
	docker-compose down
	@echo "Services stopped."

## restart: Restart all services
restart: down up

## build: Build Docker images without starting
build:
	@echo "Building Docker images..."
	docker-compose build
	@echo "Build complete."

## logs: View service logs
logs:
	docker-compose logs -f

## logs-api: View API service logs only
logs-api:
	docker-compose logs -f api

## test: Run test suite
test:
	@echo "Running tests..."
	docker-compose run --rm api pytest tests/ -v --tb=short
	@echo "Tests complete."

## test-coverage: Run tests with coverage report
test-coverage:
	@echo "Running tests with coverage..."
	docker-compose run --rm api pytest tests/ -v --cov=. --cov-report=term-missing --cov-report=html
	@echo "Coverage report generated in htmlcov/"

## test-local: Run tests locally (requires virtual environment)
test-local:
	pytest tests/ -v --tb=short

## shell: Open a shell in the API container
shell:
	docker-compose exec api /bin/bash

## db-shell: Connect to PostgreSQL
db-shell:
	docker-compose exec db psql -U kasparro -d kasparro

## etl-run: Trigger ETL run manually
etl-run:
	@echo "Triggering ETL run..."
	curl -X POST http://localhost:8000/api/etl/trigger
	@echo ""

## health: Check service health
health:
	@echo "Checking health..."
	curl -s http://localhost:8000/health | python -m json.tool
	@echo ""

## stats: Get ETL statistics
stats:
	@echo "Getting ETL stats..."
	curl -s http://localhost:8000/stats | python -m json.tool
	@echo ""

## clean: Remove all containers, volumes, and images
clean:
	@echo "Cleaning up..."
	docker-compose down -v --rmi all
	@echo "Cleanup complete."

## clean-volumes: Remove only volumes (keeps images)
clean-volumes:
	docker-compose down -v
	@echo "Volumes removed."

## lint: Run linting (requires local environment)
lint:
	@echo "Running linters..."
	ruff check .
	mypy .

## format: Format code (requires local environment)
format:
	@echo "Formatting code..."
	ruff format .
	ruff check --fix .

## setup-local: Set up local development environment
setup-local:
	@echo "Setting up local environment..."
	python -m venv venv
	@echo "Activate with: source venv/bin/activate (Linux/Mac) or venv\\Scripts\\activate (Windows)"
	@echo "Then run: pip install -r requirements.txt"
