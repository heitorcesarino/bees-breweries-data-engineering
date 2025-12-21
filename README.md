# 🐝BEES Data Engineering – Breweries Case

This project implements an end-to-end data pipeline that consumes data from the Open Brewery DB API and persists it into a data lake following the Medallion Architecture (Bronze, Silver, Gold).

The solution focuses on clean architecture, testability, and production-ready practices, simulating a real-world data engineering workflow.


## 🎯Objective

Consume brewery data from a public API

Persist raw data in a Bronze layer

Transform and curate data into a Silver layer

Create an analytical Gold layer with aggregated insights

Orchestrate the pipeline using Airflow

Ensure code quality with unit tests and modular design


## 🏗️Architecture Overview
API (Open Brewery DB)
        ↓
     Bronze Layer (Raw JSON)
        ↓
     Silver Layer (Parquet, partitioned by location)
        ↓
     Gold Layer (Aggregated analytics)


## 🥉Bronze Layer

Purpose:
Persist raw data from the API with minimal transformation.

Details:

Data is fetched using a dedicated API client

Stored as JSON files

Acts as a historical and immutable source of truth


## 🥈Silver Layer

Purpose:
Clean, normalize, and structure the raw data.

Details:

Reads JSON files from the Bronze layer

Converts data to Parquet format

Partitions data by:

country

state

city

Prepares data for analytical consumption


## 🥇Gold Layer

Purpose:
Provide business-ready aggregated data.

Aggregation:

Quantity of breweries by:

brewery_type

country

state

city

Output schema:

country

state

city

brewery_type

brewery_count


## 🛫Orchestration (Airflow)

The pipeline is orchestrated using Apache Airflow.

DAG characteristics:

Schedule: @daily

Retries: 3

Retry delay: 5 minutes

Catchup disabled

Clear dependency chain:

Bronze → Silver → Gold


The DAG is intentionally thin, delegating business logic to reusable pipeline components.


## 🧪Testing Strategy

Unit tests implemented for:

API client

Storage layers

Pipeline logic (Bronze, Silver, Gold)

External API calls are mocked

Temporary file systems (tmp_path) are used for isolation

This ensures:

High test coverage

Deterministic tests

Easy local execution


## 🛠️Tech Stack

Python 3.12

Poetry – dependency management

Pandas / PyArrow – data processing

Apache Airflow – orchestration

Pytest – testing

Pydantic – data contracts

Docker-ready architecture (optional extension)


## ▶️How to Run Locally
1️⃣ Install dependencies
poetry install

2️⃣ Run tests
poetry run python -m pytest -v

3️⃣ Run pipelines manually (example)
from bees_breweries.pipelines.bronze_pipeline import BronzeBreweriesPipeline


## 📊Monitoring & Alerting (Design)

In a production environment, monitoring would include:

Pipeline Health

Airflow task failure alerts (email / Slack)

SLA miss detection

Data Quality

Empty dataset detection

Schema validation failures

Unexpected volume changes (day-over-day)

Observability

Structured logging

Metrics on row counts per layer

Historical data lineage via Bronze persistence


## ⚖️Design Decisions & Trade-offs

Pandas over Spark: chosen for simplicity and clarity in a technical assessment.
In production, this pipeline could be migrated to Spark for scalability.

Parquet format: efficient columnar storage for analytical workloads.

Medallion architecture: ensures separation of concerns and data reliability.

Thin DAG, fat pipelines: improves testability and reusability.


## 🚀Future Improvements

Migrate Silver/Gold layers to Spark

Introduce Delta Lake

Add data validation with Great Expectations

Deploy Airflow with KubernetesExecutor

Add CI pipeline for automated testing


## 👤Author

Heitor Cesarino
Data Engineer
