# Castor Senior Data Engineer Assessment - Design Notes

## 1. Solution Overview

This solution is designed around a Medallion-style architecture using **Staging**, **Core**, and **Curated** layers in a centralized **Postgres** data warehouse, orchestrated by **Apache Airflow**.

The main objective is to build a pipeline that is:

- scalable for large-volume ingestion
- safe to rerun for the same logical date
- observable in production
- resilient to source schema evolution
- strict on data quality before loading trusted layers

---

## 2. Layered Architecture

### Staging Layer
The **Staging** layer receives raw data from the source systems with minimal transformation.

For this assessment, telemetry data is simulated with CSV files representing cloud object storage, and master data is represented as Oracle-originated reference data loaded into Postgres.

The Staging layer serves the following purposes:

- preserves raw source fidelity
- supports auditability and replay
- adds ingestion metadata such as:
  - `logical_date`
  - `ingestion_ts`
  - `source_file`

This layer is intentionally permissive so ingestion remains robust even when source structures change.

### Core Layer
The **Core** layer contains validated, normalized, and deduplicated data.

This is the main trust boundary of the warehouse. In this layer, the pipeline:

- applies data quality validations
- normalizes data types
- rejects invalid or orphaned records
- performs idempotent loading using UPSERT semantics

The Core layer ensures that only clean and governed data becomes part of the warehouse foundation.

### Curated Layer
The **Curated** layer is intended for business-facing datasets such as reporting tables, marts, and Power BI serving structures.

Its purpose is to isolate business logic from raw ingestion complexity and provide stable analytical outputs for consumers.

---

## 3. Incremental Loading Strategy

For the large-volume table receiving approximately **50 million records monthly**, I would use **incremental processing based on logical partitioning**.

The logical partition is typically based on **business date** or **event date**, depending on the source domain and reporting needs.

Instead of reloading the full dataset, the pipeline processes only the relevant partition for the Airflow execution date. This has several advantages:

- improves performance
- reduces processing time
- lowers operational risk
- makes backfills safer
- aligns with Postgres partitioning and warehouse best practices

This approach is more scalable than full reloads and is operationally safer in production environments.

---

## 4. Idempotency Strategy

A key requirement of the assessment is that the pipeline must support reruns for the same logical date without duplicating data.

To satisfy this requirement, the Core load uses **UPSERT logic** in Postgres with `ON CONFLICT DO UPDATE`.

This means that if the same record for the same business key is processed again during a rerun or backfill, the existing row is updated instead of duplicated.

This design supports production-grade rerun behavior and makes the pipeline deterministic for a given logical date.

---

## 5. Data Quality Approach

Data quality checks are enforced before data is loaded into the Core layer.

The pipeline validates:

- nulls in critical business fields such as:
  - `device_id`
  - `event_timestamp`
  - `event_date`
- orphan records by validating telemetry rows against a device master reference table

Invalid records are redirected to a reject table instead of being loaded into Core.

Additionally, the pipeline fails explicitly if more than **5 percent** of a given load contains nulls in critical columns. This protects downstream reporting and prevents silent quality degradation.

This design favors visible failure over unreliable data propagation.

---

## 6. Schema Evolution

The pipeline is designed to handle the addition of new source columns without breaking ingestion.

If new columns appear in the incoming CSV, the Staging table can be extended dynamically by adding those columns as nullable text fields.

This allows ingestion to remain resilient while preserving governance in the Core layer, where only approved and typed fields are loaded into trusted tables.

This separation helps the pipeline adapt to source-side evolution without disrupting operations.

---

## 7. Observability

The solution includes structured observability at the pipeline level.

The pipeline logs:

- start and end activity
- rows processed
- rows rejected by data quality checks
- completion metrics for Core loading

These logs make it easier to troubleshoot failures, validate load behavior, and monitor operational health.

For a production implementation, these logs could also be integrated into centralized monitoring and alerting systems.

---

## 8. SLA Monitoring

If a critical reporting pipeline must complete before **6:00 AM**, I would configure **Airflow SLA monitoring** on the critical tasks and use an **SLA miss callback** to notify the team through channels such as email, Slack, or PagerDuty.

I would also complement runtime SLA monitoring with data freshness checks in the Curated layer, since task success alone does not always guarantee that business data is available and complete.

---

## 9. Leadership Perspective

From a leadership standpoint, I would review junior engineer code with a strong focus on scalability and production safety.

For example, if a junior engineer were using `SELECT *` and loading all rows into Airflow memory, I would guide them to:

- select only required columns
- process incrementally by logical date
- avoid using Airflow as a heavy compute engine
- move transformations into the database
- implement idempotent load patterns
- add logging and measurable quality controls

This improves not only the pipeline itself, but also the long-term engineering practices of the team.

---

## 10. Conclusion

This solution is designed to be more than functionally correct. It is intended to be:

- scalable
- idempotent
- observable
- maintainable
- production-oriented

The overall design emphasizes data integrity, rerun safety, operational visibility, and clean separation of concerns across the warehouse layers.