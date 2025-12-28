# BEES Data Engineering – Breweries Case

## Overview

This repository contains an end-to-end **data engineering pipeline** that consumes data from the **Open Brewery DB API** and persists it into a data lake following the **Medallion Architecture** (Bronze, Silver, Gold).

The project was designed to be **clean, modular, testable, and production-oriented**, simulating a real-world data engineering workflow while remaining easy to run locally for evaluation purposes.

---

## Objectives

The main goals of this project are to:

- Consume brewery data from a public API
- Persist raw data in a **Bronze** layer
- Transform and curate data into a **Silver** layer
- Build an analytical **Gold** layer with aggregated insights
- Demonstrate orchestration concepts using **Apache Airflow**
- Apply good software engineering practices (tests, modularity, error handling)

This directly addresses the requirements defined in the **BEES Data Engineering – Breweries Case** technical assessment.

---

## Architecture Overview

```
Open Brewery DB API
        ↓
     Bronze Layer (Raw JSON)
        ↓
     Silver Layer (Parquet, partitioned by location)
        ↓
     Gold Layer (Aggregated analytics)
```

Each layer has a clear responsibility and is implemented as an independent pipeline component.

---

## Medallion Layers

### 🟤 Bronze Layer

**Purpose**  
Persist raw data from the API with minimal transformation.

**Details**
- Data fetched via a dedicated API client
- Stored as JSON files
- Immutable, historical source of truth

---

### ⚪ Silver Layer

**Purpose**  
Clean, normalize, and structure raw data for analytical use.

**Details**
- Reads JSON files from the Bronze layer
- Converts data to **Parquet** format
- Partitions data by:
  - country
  - state
  - city

This layer prepares the data for efficient querying and aggregation.

---

### 🟡 Gold Layer

**Purpose**  
Provide business-ready, aggregated datasets.

**Aggregation logic**
- Count of breweries grouped by:
  - brewery_type
  - country
  - state
  - city

**Output schema**
- country
- state
- city
- brewery_type
- brewery_count

---

## Orchestration Strategy

### Apache Airflow (Design)

An **Apache Airflow DAG** was implemented to demonstrate orchestration capabilities, including:

- Task dependencies (Bronze → Silver → Gold)
- Retries and retry delays
- Scheduling configuration

> **Important note**  
> Apache Airflow is **not executed locally in this project** due to native limitations on Windows environments.

### Why Airflow is not executed locally

- Airflow is officially supported only on **POSIX-compliant systems** (Linux / macOS)
- Local execution on Windows requires **WSL2 or Docker**, which would add unnecessary complexity for a technical assessment

### Design choice

- The **DAG is fully implemented and production-ready**
- Business logic lives inside reusable pipeline modules
- Pipelines are executed locally via a **`run_pipeline.py` entry point**

This approach ensures:
- Easy local execution for evaluators
- Clean separation between orchestration and business logic
- Smooth migration to a real Airflow environment if needed

---

## Running the Project Locally

### 1️⃣ Clone the repository

```bash
git clone https://github.com/heitorcesarino/bees-breweries-data-engineering.git
cd bees-breweries
```

---

### 2️⃣ Install Poetry

If you don't have Poetry installed:

```bash
pip install poetry
```

---

### 3️⃣ Install dependencies

```bash
poetry install
```

---

### 4️⃣ Activate the virtual environment

```bash
poetry shell
```

---

### 5️⃣ Run tests

```bash
poetry run pytest -v
```

All external API calls are mocked to ensure deterministic and fast tests.

---

### 6️⃣ Run the pipelines manually

The entire pipeline can be executed locally using the provided runner:

```bash
poetry run python run_pipeline.py
```

This will execute:
1. Bronze ingestion
2. Silver transformation
3. Gold aggregation

---

## Monitoring & Alerting (Design Proposal)

In a production environment, monitoring and alerting would include:

### Pipeline Health
- Task failure alerts (Slack / Email)
- Retry and SLA miss detection

### Data Quality
- Empty dataset detection
- Schema validation
- Unexpected volume changes (day-over-day)

### Observability
- Structured logging
- Row count metrics per layer
- Historical traceability via Bronze persistence

---

## Tech Stack

- **Python 3.12**
- **Poetry** – dependency and environment management
- **Pandas / PyArrow** – data processing
- **Apache Airflow** – orchestration (design-level)
- **Pytest** – unit testing
- **Pydantic** – configuration and data contracts

---

## Design Decisions & Trade-offs

- **Pandas over Spark**: chosen for simplicity and clarity in a technical assessment. The architecture allows an easy migration to Spark.
- **Parquet format**: efficient columnar storage for analytics.
- **Medallion architecture**: ensures separation of concerns and data reliability.
- **Thin DAG, fat pipelines**: improves testability and reusability.

---

## Future Improvements

- Migrate Silver and Gold layers to Spark
- Introduce Delta Lake
- Add data validation with Great Expectations
- Deploy Airflow using Docker / Kubernetes
- Add CI pipeline for automated testing

---

## Author

**Heitor Cesarino**  
Data Engineer

