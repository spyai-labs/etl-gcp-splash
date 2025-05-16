# GCP Splash API ETL Pipeline

This project implements a production-ready, scalable ETL pipeline to extract event and group contact data from the Splash API, transform it with entity-specific logic, and load it into Google BigQuery. It is designed to run on **Google Cloud Run**, with logging and metadata persisted to **Google Cloud Storage** and **BigQuery** for traceability.

---

## 🔧 Features

- **Entity-based Extractors**: `EventExtractor`, `GroupContactExtractor`, supporting paginated APIs with date filters and nested JSON parsing.
- **Flexible Sync Modes**:
  - `incremental`: Hourly or recent lookback syncs
  - `incremental_window`: Date-ranged syncs
  - `historical_full`: Full backfill since 2023-01-01
- **Modular Transformers**: Entity-specific transformation logic with reusable base classes.
- **Robust Loader**: Deduplicated merge using BigQuery `MERGE` statements.
- **Secure Auth**: Splash OAuth token managed via Google Secret Manager.
- **Comprehensive Logging**: Rotating file logs, Cloud Logging, GCS archive upload, and BigQuery job status tracking.
- **Thoroughly Tested**: Unit tests across all modules with `pytest`.

---

## 📁 Project Structure

```
gcp-splash-api/
├── Dockerfile
├── README.md
├── requirements.txt
└── src/
    ├── main.py                        # Entry point for ETL execution
    ├── splash/
    │   ├── auth.py                    # Splash token auth via GCP Secret Manager
    │   ├── config/                    # Environment setup and validation
    │   ├── defined_types/             # Type definitions for ETL, job stats, BQ schema
    │   ├── extractor/                 # Event & GroupContact extractors with base class
    │   ├── loader/                    # BigQuery loader and merge logic
    │   ├── metadata/                  # ETL job metadata tracking
    │   ├── model/                     # Pydantic models for schema generation
    │   ├── schema/                    # BigQuery schemas
    │   ├── sync_controller.py         # Sync mode-driven date window controller
    │   ├── transformer/               # Modular transformers and map
    │   ├── utils/                     # Utility modules: logging, requests, time, etc.
    │   └── secret/                    # GCP integrations to Secret Manager
    └── tests/                         # pytest-based test suite
```

---

## 🚀 Usage

### 1. Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Set the following `.env` or runtime variables:

- `SYNC_MODE=incremental | incremental_window | historical_full`
- `START_DATE=2024-01-01`  # only needed for 'incremental_window' sync_mode
- `END_DATE=2024-01-31`    # only needed for 'incremental_window' sync_mode
- `SPLASH_ETL_SOURCES=event,group_contact`
- `GCP_PROJECT_ID=...`
- `TOKEN_SECRET_ID=splash-token`
- (Others defined in `settings.py`)

### 3. Run Locally

```bash
python src/main.py
```

### 4. Run in Docker

```bash
docker build -t gcp-splash-api .
docker run --env-file .env gcp-splash-api
```

---

## ✅ Testing

Run all tests:

```bash
pytest src/tests
```

Sample test files include:
- `test_main.py` – ETL orchestration
- `test_event_extractor.py` – API pagination & filtering
- `test_auth.py` – Token refresh logic
- `test_logger.py` – Rotating file + stream + propagate behavior

---

## 📊 Logging & Monitoring

- Logs are saved in `/tmp/<logger>.log` and uploaded to GCS if `ENABLE_GCS_LOGS=true`
- Job statuses logged to BigQuery if `ENABLE_BQ_LOGS=true`
- Logs also propagate to root logger (supports `pytest caplog` and Cloud Logging)

---

## 🔐 Authentication

- OAuth tokens are retrieved from **Google Secret Manager**
- Automatically refreshed using `refresh_token`
- New tokens are persisted back to Secret Manager with timestamped rotation

---

## 🧩 ETL Sync Modes

| Mode                | Description                                  | Use Case                                                 |
|---------------------|----------------------------------------------|----------------------------------------------------------|
| `incremental`       | Time-based rolling window (e.g. last 7d)     | Scheduled hourly/daily sync (does not capture deletions) |
| `incremental_window`| Explicit date range (START_DATE to END_DATE) | Ad hoc or backfill sync (does not capture deletions)     |
| `historical_full`   | From fixed start (e.g. 2023-01-01) to now    | One-off historical sync (captures all deletions)         |

---

## 👨‍🔧 Contributors

Written by @spyai-dev

---

## 📜 License

MIT License. See `LICENSE` file.

---

## 🚀 Deployment

Refer to the sample `Dockerfile`.
