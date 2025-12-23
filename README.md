# Kasparro Backend & ETL System

A production-grade backend ETL system with multi-source data ingestion, RESTful API, and comprehensive observability.

## 🚀 Quick Start

```bash
# Start all services
make up

# Run tests
make test

# Stop services
make down
```

## 📋 Features

### P0 - Foundation
- ✅ Multi-source ETL (API + CSV)
- ✅ PostgreSQL storage (raw + normalized)
- ✅ Pydantic validation
- ✅ Incremental ingestion
- ✅ RESTful API with pagination & filtering
- ✅ Health endpoint with DB/ETL status

### P1 - Growth
- ✅ Third data source (RSS feed)
- ✅ Checkpoint-based resume
- ✅ Idempotent writes
- ✅ /stats endpoint
- ✅ Comprehensive tests

### P2 - Differentiator
- ✅ Schema drift detection
- ✅ Failure injection & recovery
- ✅ Rate limiting with backoff
- ✅ Prometheus metrics
- ✅ Structured JSON logging
- ✅ Run comparison endpoints

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│    API Source   │    │   CSV Source    │    │   RSS Source    │
└────────┬────────┘    └────────┬────────┘    └────────┬────────┘
         │                      │                      │
         └──────────────────────┼──────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │    ETL Pipeline       │
                    │  - Fetch             │
                    │  - Validate          │
                    │  - Transform         │
                    │  - Load              │
                    └───────────┬───────────┘
                                │
                    ┌───────────▼───────────┐
                    │      PostgreSQL       │
                    │  - raw_api_data      │
                    │  - raw_csv_data      │
                    │  - raw_rss_data      │
                    │  - unified_data      │
                    │  - etl_checkpoints   │
                    │  - etl_runs          │
                    └───────────┬───────────┘
                                │
                    ┌───────────▼───────────┐
                    │      FastAPI          │
                    │  GET /data            │
                    │  GET /health          │
                    │  GET /stats           │
                    │  GET /metrics         │
                    └───────────────────────┘
```

## 🔧 Configuration

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://...` |
| `API_KEY` | External API authentication key | Required |
| `ETL_INTERVAL_MINUTES` | ETL run frequency | `5` |
| `LOG_LEVEL` | Logging level | `INFO` |

## 📡 API Endpoints

### GET /health
Health check with DB and ETL status.

```bash
curl http://localhost:8000/health
```

### GET /data
Fetch normalized data with pagination and filtering.

```bash
curl "http://localhost:8000/data?limit=10&offset=0&source=api"
```

### GET /stats
ETL run statistics and summaries.

```bash
curl http://localhost:8000/stats
```

### GET /metrics
Prometheus-format metrics.

```bash
curl http://localhost:8000/metrics
```

## 🧪 Testing

```bash
# Run all tests
make test

# Run with coverage
make test-coverage

# Run specific test file
docker-compose run --rm api pytest tests/test_api/ -v
```

## 📁 Project Structure

```
kasparro_backend/
├── api/                    # FastAPI application
│   ├── routes/             # API endpoints
│   └── dependencies.py     # Shared dependencies
├── ingestion/              # ETL pipeline
│   ├── sources/            # Data source connectors
│   └── pipeline.py         # Orchestration
├── services/               # Business logic
├── schemas/                # Pydantic models
├── core/                   # Configuration
├── tests/                  # Test suite
├── migrations/             # Database migrations
└── data/                   # Sample CSV files
```

## 🚢 Deployment

### Local Docker
```bash
make up
```

### AWS Deployment
See `docs/deployment.md` for AWS ECS deployment guide.

## 📄 License

MIT
