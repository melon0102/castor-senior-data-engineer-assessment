# Castor - Senior Data Engineer Technical Assessment

## Overview

This repository contains my solution for the Castor Senior Data Engineer technical assessment.

The project implements an incremental data pipeline using **Apache Airflow** and **Postgres**, following a Medallion-style architecture with **Staging**, **Core**, and **Curated** layers.

The solution is designed to demonstrate:

- incremental loading by logical date
- idempotent reruns using UPSERT
- data quality validation before Core loading
- schema evolution handling in Staging
- observability through structured logging and SLA-oriented design

---

## Architecture

This solution uses a layered warehouse design:

### Staging
The Staging layer receives raw source data with minimal transformation.

For this assessment:
- telemetry data is simulated with CSV files representing cloud object storage
- master data is represented through reference tables in Postgres

Staging preserves raw data and adds ingestion metadata such as:
- `logical_date`
- `ingestion_ts`
- `source_file`

### Core
The Core layer contains validated, normalized, deduplicated, and trusted data.

This layer is responsible for:
- data quality validation
- normalization of data types
- rejection of invalid rows
- idempotent loading using UPSERT logic

### Curated
The Curated layer is intended for business-facing tables, marts, and reporting datasets, such as Power BI consumption models.

---


## Design Decisions

### Incremental Load Strategy
The pipeline processes data by **logical date** instead of reloading the full dataset.

For large tables receiving high monthly volume, this approach:
- improves performance
- reduces runtime
- lowers operational risk
- makes backfills safer
- aligns with logical partitioning patterns in a warehouse environment

### Idempotency
The Core layer uses **Postgres UPSERT** with `ON CONFLICT DO UPDATE`.

This allows the DAG to be rerun multiple times for the same Airflow logical date without duplicating records.

### Data Quality
Before loading into Core, the pipeline validates:
- nulls in critical columns such as `device_id`, `event_timestamp`, and `event_date`
- orphan records by checking telemetry data against the device master

Invalid rows are redirected to a reject table.

The pipeline fails explicitly if more than **5%** of the rows in a load contain nulls in critical fields.

### Schema Evolution
If new columns appear in the source CSV, the Staging layer can add them dynamically as nullable `TEXT` columns.

This prevents ingestion from breaking due to source-side schema changes, while preserving governance in the Core layer.

### Observability
The pipeline logs:
- task execution activity
- rows processed
- rows rejected
- Core load completion metrics

This provides operational visibility and supports troubleshooting.

---

## Repository Structure

```text
castor-senior-data-engineer-assessment/
├── dags/
│   └── telemetry_ingestion_dag.py
├── plugins/
│   ├── __init__.py
│   ├── hooks/
│   │   ├── __init__.py
│   │   └── csv_hook.py
│   └── operators/
│       ├── __init__.py
│       ├── data_quality_operator.py
│       ├── extract_csv_to_stage_operator.py
│       └── upsert_core_operator.py
├── sample_data/
│   └── telemetry_2026-04-15.csv
├── sql/
│   └── create_tables.sql
├── DESIGN_NOTES.md
├── README.md
└── requirements.txt
