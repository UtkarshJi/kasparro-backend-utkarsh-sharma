# Kasparro Backend & ETL System

A production-grade backend ETL system for cryptocurrency data with multi-source ingestion, **identity unification**, RESTful API, and cloud deployment.

## 🌐 Live Demo

**API:** https://kasparro-api-18sg.onrender.com

| Endpoint | URL |
|----------|-----|
| Health | https://kasparro-api-18sg.onrender.com/health |
| Data | https://kasparro-api-18sg.onrender.com/data?limit=10 |
| Stats | https://kasparro-api-18sg.onrender.com/stats |
| Docs | https://kasparro-api-18sg.onrender.com/docs |

## 🚀 Quick Start

```bash
# Clone repository
git clone https://github.com/UtkarshJi/kasparro-backend-utkarsh-sharma.git
cd kasparro-backend-utkarsh-sharma

# Start all services
make up

# Run tests
make test

# View logs
make logs

# Stop services
make down
```

## 📋 Features

### Data Sources
- **CoinPaprika API** - Cryptocurrency ticker data (2000+ coins)
- **CoinGecko API** - Market data with prices and rankings
- **CSV** - Product data ingestion

### 🔗 Identity Unification (NEW)
Same cryptocurrency from different sources (CoinPaprika + CoinGecko) is **unified into a single record** using symbol-based canonical IDs:
- Bitcoin from both sources → `canonical_id='btc'` → **1 unified record**
- No duplicate entries for the same coin
- Cross-source data merging

### P0 - Foundation ✅
- Multi-source ETL (CoinPaprika + CoinGecko + CSV)
- PostgreSQL storage (raw + normalized)
- Pydantic validation with type cleaning
- Incremental ingestion with checkpoints
- RESTful API with pagination & filtering
- Health endpoint with DB/ETL status

### P1 - Growth ✅
- Third data source (CSV)
- Checkpoint-based resume on failure
- Idempotent writes (upserts)
- `/stats` endpoint with analytics
- Comprehensive test suite (76 tests)

### P2 - Differentiator ✅
- **Identity unification** across data sources
- Schema drift detection with fuzzy matching
- Failure recovery with checkpoints
- Rate limiting with exponential backoff
- Prometheus metrics (`/metrics`)
- Structured JSON logging
- Run comparison endpoints

## 🏗️ Architecture

```
┌───────────────────┐  ┌───────────────────┐  ┌───────────────────┐
│   CoinPaprika     │  │    CoinGecko      │  │       CSV         │
│   (API 1)         │  │    (API 2)        │  │    (Products)     │
└─────────┬─────────┘  └─────────┬─────────┘  └─────────┬─────────┘
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                     ┌───────────▼───────────┐
                     │    ETL Pipeline       │
                     │  • Fetch with retry   │
                     │  • Validate (Pydantic)│
                     │  • Transform          │
                     │  • Identity Resolver  │ ← Generates canonical_id
                     │  • Upsert by canon_id │ ← Merges same coins
                     └───────────┬───────────┘
                                 │
                     ┌───────────▼───────────┐
                     │      PostgreSQL       │
                     │  • raw_api_data       │
                     │  • raw_csv_data       │
                     │  • unified_data       │ ← canonical_id unique
                     │  • etl_checkpoints    │
                     │  • etl_runs           │
                     └───────────┬───────────┘
                                 │
                     ┌───────────▼───────────┐
                     │       FastAPI         │
                     │  GET /data            │
                     │  GET /health          │
                     │  GET /stats           │
                     │  GET /metrics         │
                     │  POST /api/etl/trigger│
                     └───────────────────────┘
```

## 📡 API Endpoints

### GET /health
Health check with DB and ETL status.
```bash
curl https://kasparro-api-18sg.onrender.com/health
```

### GET /data
Fetch cryptocurrency data with pagination and filtering.
```bash
curl "https://kasparro-api-18sg.onrender.com/data?limit=10&source=coinpaprika"
```

### GET /stats
ETL run statistics and per-source breakdowns.
```bash
curl https://kasparro-api-18sg.onrender.com/stats
```

### GET /metrics
Prometheus-format metrics.
```bash
curl https://kasparro-api-18sg.onrender.com/metrics
```

### POST /api/etl/trigger
Manually trigger ETL run.
```bash
curl -X POST https://kasparro-api-18sg.onrender.com/api/etl/trigger
```

## 🔧 Configuration

Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | Auto-converted for asyncpg |
| `COINPAPRIKA_API_KEY` | Optional API key | Not required |
| `ETL_INTERVAL_MINUTES` | ETL run frequency | `5` |
| `ETL_BATCH_SIZE` | Records per batch | `50` |
| `LOG_LEVEL` | Logging level | `INFO` |

## 🧪 Testing

```bash
# Run all tests (76 tests)
make test

# Run with coverage
make test-coverage

# Run specific test file
docker compose run --rm api pytest tests/test_etl/ -v
```

## 📁 Project Structure

```
kasparro_backend/
├── api/                    # FastAPI application
│   ├── routes/             # API endpoints
│   └── dependencies.py     # Shared dependencies
├── ingestion/              # ETL pipeline
│   ├── sources/            # Data source connectors
│   │   ├── coinpaprika_source.py
│   │   ├── coingecko_source.py
│   │   └── csv_source.py
│   └── pipeline.py         # Orchestration
├── services/               # Business logic
│   ├── identity_resolver.py # Cross-source ID unification
│   ├── rate_limiter.py     # Token bucket
│   └── schema_drift.py     # Drift detection
├── schemas/                # Pydantic models
├── core/                   # Configuration
├── tests/                  # Test suite (76 tests)
└── data/                   # Sample CSV files
```

## 🚢 Deployment

### Local Docker
```bash
make up
# API available at http://localhost:8000
```

### Cloud (Render)
The application is deployed on Render with:
- PostgreSQL database
- Docker web service
- Auto-deploy from GitHub
- Scheduled ETL every 5 minutes
- Automatic schema migration

## 📊 Tech Stack

- **Framework:** FastAPI
- **Database:** PostgreSQL + SQLAlchemy (async)
- **ETL:** Custom pipeline with APScheduler
- **Identity Unification:** Symbol-based canonical IDs
- **Validation:** Pydantic
- **Testing:** Pytest (76 tests)
- **Logging:** Structlog (JSON format)
- **Metrics:** Prometheus
- **Container:** Docker + Docker Compose

## 👤 Author

**Utkarsh Sharma**

## 📄 License

MIT

