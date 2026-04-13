# Sentinel-AI: Enterprise Energy Analytics Pipeline

A production-grade ELT pipeline architected for the modern defense and energy ecosystem.

## 🚀 Architecture
- **Producer:** Python/UUID-driven historical data generator (2021-2026).
- **Orchestration:** Apache Airflow 3.0 (Dockerized).
- **Storage:** AWS S3 (Landing Zone) & Snowflake (Data Warehouse).
- **Transformation:** dbt (Silver & Gold Layers) with automated Data Quality testing.
- **Visualization:** Power BI (DirectQuery with RLS).

## 🛠️ Data Governance
Every batch undergoes 6+ automated integrity tests:
- UUID Uniqueness enforcement.
- Non-negative consumption validation (dbt-utils).
- Status code accepted-value constraints.

## 📈 Roadmap
- [x] S3-to-Snowflake Auto-ingest (Snowpipe)
- [x] dbt Silver Layer & Testing
- [ ] Gold Layer dimensional modeling (Star Schema)
- [ ] Power BI Service Deployment with RLS