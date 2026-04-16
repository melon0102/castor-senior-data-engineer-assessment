# Castor - Senior Data Engineer Technical Assessment

## Overview
This repository contains a sample implementation for the Castor Senior Data Engineer technical assessment.

The solution is based on a Medallion-style architecture using:
- **Staging** for raw ingestion
- **Core** for validated and normalized records
- **Curated** for business-serving data marts

The pipeline is orchestrated with **Apache Airflow** and loads data into **Postgres**.

It demonstrates:
- incremental processing by logical date
- idempotent reruns using UPSERT
- data quality checks
- schema evolution handling for new source columns
- structured observability and SLA design

---

## Architecture Summary

### Staging (Bronze)
Raw ingestion zone. Keeps source fidelity and adds ingestion metadata.

### Core (Silver)
Validated, normalized, deduplicated data. Business keys are enforced here.

### Curated (Gold)
Business-ready tables and views for BI tools such as Power BI.

---

## Mermaid Diagram

```mermaid
flowchart TD
    A[Telemetry Files in S3 / Blob Storage] --> B[Airflow Ingestion DAG]
    A2[Oracle Master Data] --> B2[Airflow Oracle Extract DAG]

    B --> C[Staging Schema / Bronze Tables]
    B2 --> C

    C --> D[Schema Validation]
    D --> E[Data Quality Checks]
    E --> F[Reject / Quarantine Table]
    E --> G[Core Load / Silver Tables]

    G --> H[Deduplication + UPSERT by Business Key]
    H --> I[Partitioned Core Tables]

    I --> J[Curated Transformations / Gold]
    J --> K[Power BI Serving Tables / Views]

    B --> L[Structured Logs]
    B2 --> L
    E --> L
    G --> L
    J --> L

    L --> M[Monitoring / SLA Alerts]
