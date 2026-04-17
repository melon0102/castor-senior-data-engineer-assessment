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

```markdown
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
```

## Assumptions

The implementation is based on the following assumptions:

- Airflow provides the orchestration layer and passes a `logical_date` for each run.
- Source telemetry files arrive daily and are partitioned by date.
- The destination warehouse is hosted in Postgres.
- Device master data is already available in Postgres and can be used for referential validation.
- Reprocessing the same logical date is a valid operational scenario and must not create duplicates.
- Curated models are downstream-facing and can be extended for BI tools such as Power BI.

## Pipeline Flow

At a high level, the pipeline follows this sequence:

1. Detect and load the source CSV corresponding to the Airflow logical date.
2. Ensure the Staging table can receive all source columns, including newly added ones.
3. Insert raw records into Staging along with ingestion metadata.
4. Run Data Quality validations against critical business fields.
5. Redirect invalid rows to a reject table for traceability.
6. Fail the pipeline if invalid critical rows exceed the allowed threshold.
7. Load valid records into the Core layer using UPSERT semantics.
8. Emit structured logs with execution metrics for monitoring and support.

This sequence keeps ingestion flexible while protecting the trusted Core model.

## Data Model Notes

### Example Business Keys

For telemetry data, the business key may be based on a composite identifier such as:

- `device_id`
- `event_timestamp`
- `event_type`

This key is used to guarantee deterministic UPSERT behavior in Core.

### Metadata Columns

The Staging layer stores technical metadata to support replayability and lineage, including:

- `logical_date`
- `ingestion_ts`
- `source_file`
- `load_id` or `run_id` (optional future enhancement)

These fields make it easier to audit ingestion activity and investigate downstream issues.

## Error Handling Strategy

The pipeline is designed to fail early when data integrity is at risk, while still preserving enough context for troubleshooting.

### Current error handling approach

- Missing source file for a logical date causes the DAG to fail explicitly.
- Invalid critical rows are captured in a reject table.
- Loads into Core are executed only for rows that pass validation.
- If the null-rate threshold exceeds 5%, the process fails intentionally.
- Structured logs provide row counts and execution details for diagnosis.

### Why this matters

This approach balances resilience with data trust. The pipeline does not silently ignore data quality issues that could contaminate downstream BI reporting.

## Backfill Strategy

This solution is designed to support historical reprocessing safely.

### Backfill behavior

- A DAG run can be triggered for any past logical date.
- The extraction step reads only the partition associated with that logical date.
- Core loads remain safe because the write pattern is idempotent.
- Reject records are still traceable for each backfill execution.

This allows the team to replay missed or corrected source data without requiring manual cleanup.

## Performance Considerations

Even though this assessment uses a simulated CSV input, the design is intended to scale to production-like workloads.

### Performance-oriented decisions

- Process by logical partition instead of full refresh.
- Avoid loading entire historical datasets on each run.
- Minimize transformation work inside Airflow itself.
- Delegate persistence and merge logic to Postgres.
- Isolate trusted and untrusted data into separate layers.

### Production scaling considerations

For higher throughput environments, I would additionally consider:

- Bulk loading techniques such as `COPY`
- Physical partitioning in Postgres for large fact-like tables
- Indexing strategy aligned with UPSERT conflict keys
- Batch or chunk processing for very large files
- Workload isolation between ingestion and reporting schemas

## Airflow Design Notes

The DAG is intentionally structured to keep orchestration clean and responsibilities separated.

### Principles used

- Airflow coordinates work, but does not become the heavy processing engine.
- Extraction, validation, and loading are isolated into reusable components.
- Custom Hooks and Operators improve readability and reuse.
- Logical date drives deterministic execution behavior.
- The pipeline is designed to be rerunnable and testable.

### Operational features considered

- Retry-safe behavior
- Task-level logging
- SLA awareness
- Backfill support
- Explicit task boundaries

## SLA Monitoring Approach

A critical requirement in a BI environment is ensuring datasets are ready before business users start consuming dashboards.

### Proposed SLA strategy

For pipelines feeding Power BI, I would configure:

- Airflow task `sla` definitions on the final critical load tasks
- `on_failure_callback` or SLA-miss callbacks for notifications
- Alert routing to email, Slack, or Teams depending on operational standards

### Example operational rule

If the curated load required for reporting is not complete by 6:00 AM, the system should:

- Raise an SLA miss event
- Notify the support or data platform team
- Include the impacted DAG, logical date, and task details in the alert

This reduces time-to-detection for issues that affect business reporting.

## Data Quality Rules Implemented

The current pipeline includes the following quality controls:

- Primary business identifiers must not be null
- Required event timestamps must not be null
- Date-derived fields must be valid
- Orphan telemetry rows are detected against the device master
- Invalid rows are isolated before Core loading
- The pipeline fails if invalid critical-field rows exceed 5%

These checks enforce minimum trust requirements before the data becomes analytically usable.

## Rejected Data Handling

Instead of discarding bad records silently, the solution preserves them for traceability.

### Reject handling goals

- Allow investigation of source data issues
- Support communication with upstream system owners
- Preserve evidence of why rows were excluded
- Avoid blocking trusted data from progressing when the issue is below threshold

### Recommended reject table attributes

A reject table may include:

- Source row payload
- Rejection reason
- Logical date
- Ingestion timestamp
- Source file
- Validation rule name

This pattern improves operational transparency and accelerates remediation.

## Code Review Guidance

If reviewing a junior engineer’s implementation for this same pipeline, I would focus on both correctness and engineering maturity.

### Main review themes

- Avoid `SELECT *`
- Avoid reading full datasets into memory unnecessarily
- Make business keys explicit
- Separate orchestration from processing
- Validate assumptions around nulls, uniqueness, and referential integrity
- Log operational metrics consistently
- Design for reruns and backfills from the start

### Coaching perspective

The purpose of the review would not only be to fix the code, but also to reinforce production-grade habits around scalability, observability, and data trust.

## Local Testing Recommendations

To validate the solution locally, the following scenarios are recommended:

### Functional tests

- Run the DAG for a valid logical date
- Rerun the same logical date and confirm no duplicates in Core
- Introduce nulls in critical fields and verify reject behavior
- Exceed the 5% threshold and verify that the DAG fails
- Add a new column to the CSV and verify Staging adapts without breaking

### Outcome

These tests validate the core acceptance criteria of the assessment:

- Idempotency
- DQ enforcement
- Schema evolution tolerance
- Observability
- Rerun safety

## Limitations

This implementation is intentionally scoped for the assessment and does not aim to be a full production platform.

### Current limitations

- Source ingestion is simulated with local CSV files
- Oracle extraction is represented conceptually rather than fully implemented
- Curated business marts are outlined but not deeply expanded
- Alert integrations may be described rather than fully wired into external systems
- Metadata management and lineage are limited to logging and technical columns

These decisions were made to prioritize the assessment’s required criteria while keeping the implementation focused and clear.

## Future Enhancements

If extended beyond the scope of the assessment, I would prioritize the following improvements:

- Real ingestion from S3 / Azure Blob Storage
- Oracle incremental extraction patterns
- Data contracts for schema governance
- Automated test coverage for operators and transformations
- CI/CD for DAG and SQL deployment
- Centralized monitoring dashboards
- Lineage and metadata tracking
- Curated dimensional models optimized for BI workloads

## Final Notes

This implementation is designed to reflect not only technical execution, but also senior-level engineering judgment in areas such as:

- Layered architecture
- Safe reprocessing
- Operational observability
- Data quality enforcement
- Incremental design
- Maintainability and team scalability

The overall goal is to provide a practical and production-minded foundation for a centralized BI data platform on Postgres.

