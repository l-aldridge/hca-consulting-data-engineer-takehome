# Part 3 — Design Proposal: Nightly PSI-13 ETL (Rolling 12-Month Window)

## Overview
This design describes a simple nightly ETL on Google Cloud Platform using **BigQuery** as the analytics warehouse and **Cloud Composer (Airflow)** for orchestration. The pipeline computes **PSI-13 Postoperative Sepsis** metrics for a **rolling 12-month window** (current month + prior 11 months), using **discharge date** to assign discharges to months.

The design prioritizes:
- explainability, correctness, and reproducibility for internal analytics and quality teams
- operational robustness with minimal unnecessary complexity

---

## Architecture & Storage Layout

### BigQuery datasets
- **`stg`**  
  Incremental staging tables populated nightly from the source warehouse. These tables capture raw clinical facts with minimal transformation and include extract metadata.

- **`core`**  
  Cleaned, modeled fact tables:
  - `fct_discharge`
  - `fct_diagnosis`
  - `fct_procedure`  
  Tables are partitioned by discharge date (or month) to support efficient rolling-window recomputation.

- **`reference`**  
  Versioned reference data including:
  - appendix code mappings
  - PSI-13 sepsis diagnosis codes
  - date dimension  
  Reference tables include effective dates and version identifiers to support historical reproducibility.

- **`analytics`**  
  Analytics-ready outputs:
  - `psi13_monthly_metrics` (authoritative monthly rates)
  - `psi13_numerator_patients` (patient-level audit detail)
  - `etl_run_log` (pipeline execution metadata)

---

**Reference data**
- Annual .gov appendix files and PSI-13 sepsis code list are manually downloaded and stored in **Google Cloud Storage**
- Loaded into BigQuery as **versioned reference tables** with effective date ranges
- Metrics join reference data based on **discharge date**, ensuring historical reproducibility

---

## Orchestration & Scheduling

- Cloud Composer runs a **nightly DAG** (e.g., 2:00 AM local time)
- The DAG is parameterized by execution date and computes metrics for the rolling 12-month window

### High-level task flow
1. Incremental extract from the source warehouse (with a small lookback buffer for late corrections ex. 7 days)
2. Load extracted data into staging tables
3. `MERGE` staged data into core fact tables (idempotent updates)
4. Validate reference data freshness
5. Recompute PSI-13 denominator eligibility and numerator membership for the rolling 12 months
6. Upsert monthly metrics and numerator patient list
7. Run data quality checks and emit alerts if necessary

---

## Reference Data Versioning & Freshness Alerts

- Annual appendix files and PSI-13 sepsis code lists are manually downloaded and archived in Cloud Storage
- Reference tables include:
  - `ref_version` (e.g., `2025-01-01`)
  - `effective_start_date` / `effective_end_date`
  - `is_active`
- PSI-13 logic joins reference data based on **discharge date**, ensuring that historical results remain stable even when definitions change

### Freshness validation
Each nightly run checks whether:
- the active reference version is appropriate for the current calendar year, or
- the effective start date of the active reference version is less than or equal to the current date

If reference data is **out of date** (e.g., still using the prior year’s appendix files after January 1):
- the pipeline emits a **non-fatal alert** (email/Slack)
- metrics continue to run using the last known valid reference
- the alert clearly signals that **manual intervention is required**

---

## Monthly Outputs & Auditing Strategy

### `analytics.psi13_monthly_metrics`
This table stores the **authoritative PSI-13 results** used by dashboards and downstream analysis.

- Grain: one row per reporting month
- Always contains **at most 12 rows** (rolling window)
- Updated nightly via `MERGE` keyed by `year_month`
- Represents the **current, best-known values** for each month

columns include:
- `year_month`
- `numerator`
- `denominator`
- `rate`
- `run_id`
- `computed_at`
- `ref_version`

---

### `analytics.psi13_numerator_patients`
This table persists the **patient-level membership of the PSI-13 numerator** and serves as the primary audit and explainability artifact.

- Grain: `patient_id` × reporting month (or discharge date)
- Includes the qualifying sepsis diagnosis code and relevant metadata
- Rows are retained across runs to allow run-to-run comparison


columns include:
- `patient_id`
- `discharge_date`
- `year_month`
- `qualifying_sepsis_code`
- `qualifying_sepsis_code_sk`
- `is_principal_diag`
- `is_present_on_admission`
- `ref_version`
- `run_id`
- `computed_at`

This table enables analysts to:
- identify exactly which discharges drove a numerator change
- diff numerator membership between ETL runs
- support clinical review and validation workflows

The numerator patient list is persisted because it is:
- relatively small
- frequently questioned when rates change

---

### `analytics.etl_run_log`
This table captures **pipeline execution metadata** and provides the backbone for traceability.

Typical fields:
- `run_id`
- `execution_date`
- `window_start` / `window_end`
- `ref_version`
- record counts (numerator, denominator)
- data quality status
- pipeline status and timestamps

The run log allows any monthly metric to be traced back to:
- the specific ETL run that produced it
- the reference definitions in effect at the time

---

## Why Denominator Membership Is Not Persisted (by Default)

Because stakeholders are primarily **internal analytics**, the design intentionally does **not** persist a full `psi13_denominator_patients` table.

Rationale:
- Denominator populations are large and can be deterministically reconstructed from core facts
- Most investigative questions focus on **which cases were flagged**, not the full eligible population
- Denominator counts and metadata are still captured per run in `etl_run_log`

If denominator volatility becomes a recurring issue, denominator membership or exclusion-reason counts can be added incrementally without redesigning the pipeline.

---

## Monitoring, Data Quality, & Alerting

Automated checks include:
- numerator ≤ denominator for each month
- exactly 12 months present in the rolling window
- no unexpected NULLs in critical fields
- reasonable month-over-month changes
- reference data freshness validation


The data pipeline applies **layer-appropriate testing** to ensure correctness, reliability, and safe re-execution. Validation intensity increases as data progresses from raw ingestion to analytics-ready outputs.

| Pipeline Layer     | Testing Focus                | Key Validations                                                                                                                    | Tools / Approach                                                                               |
| ------------------ | ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| **Raw (External)** | Structural integrity         | Schema alignment, CSV parsing, non-zero row counts                                                                                 | BigQuery external table DDL, lightweight SQL checks                                            |
| **Core (Modeled)** | Data correctness & integrity | Primary key uniqueness, foreign key resolution, UNKNOWN handling, domain rules (e.g., principal diagnosis logic), date consistency | BigQuery SQL, dbt generic tests (`not_null`, `unique`, `relationships`), custom SQL assertions |
| **Analytics**      | Aggregation accuracy         | Reconciliation to core facts, array field integrity, duplicate prevention                                                          | BigQuery SQL validation queries                                                                |
| **All Layers**     | Operational safety           | Idempotent execution, rerun safety, deterministic surrogate keys                                                                   | Idempotent DDL/DML patterns, dbt full refresh                                                  |

### Key Principles

- **Fail fast at ingestion** for structural issues.
- **Enforce business rules in the core layer**, where canonical facts and dimensions are defined.
- **Validate aggregates at the analytics layer** to ensure trustworthy reporting.
- **Design for idempotency** to support safe retries, CI/CD, and rapid iteration.

### Outcome

This testing strategy balances rigor with pragmatism, delivering **trustworthy, production-ready data** while supporting accelerated development and iterative workflows.

 Alerts are emitted for:
- DAG or task failures
- data quality violations
- stale reference data



---

## Backfills, Reruns, & Archiving

- All transformations are **idempotent**, enabling safe reruns
- Backfills are supported via DAG parameters (e.g., `start_month`, `end_month`)
- Reference files are archived in Cloud Storage by version
- Monthly metrics and numerator membership are retained in BigQuery for audit and analysis

This approach ensures that historical results can be explained and reproduced without requiring raw data re-extraction.

---

## Tradeoffs Considered

- **Rolling-window recomputation vs. incremental month updates**  
  Recomputing all 12 months nightly simplifies correctness and late-data handling at modest compute cost.

- **Versioned reference data vs. in-place updates**  
  Versioning increases modeling complexity but is essential for auditability of quality measures.

- **Persisting numerator but not denominator membership**  
  This balances storage cost and operational simplicity against explainability needs for internal analytics.

- **Alerting vs. failing on stale reference data**  
  Non-fatal alerts prevent silent drift while avoiding unnecessary pipeline downtime.

---

## Mermaid Architecture Diagram

```mermaid
flowchart LR
    %% Orchestration Layer
    subgraph Orchestration ["Orchestration (Scheduling & Control)"]
        Composer["Cloud Composer"]
        DAG["Nightly DAG"]
        Composer --> DAG
    end

    %% Data Flow Layer
    subgraph "Data Flow"
        SourceWarehouse[("Source Data Warehouse")]
        Staging["BigQuery 
        stg_*"]
        GCS[("Cloud Storage
        Annual Reference Files")]
        Reference["BigQuery
        reference.*"]
        Core["BigQuery
        core.fct_*"]
        Analytics["BigQuery
        analytics.psi13_*"]
        Alerts["Monitoring & Alerts"]
    end

    %% Orchestration triggers data flow
    DAG -.-> Staging
    DAG -.-> Analytics
    DAG -.-> Alerts

    %% Data flow
    SourceWarehouse -- "Incremental Extract" --> Staging
    GCS -- "Annual Load" --> Reference
    Staging -- "MERGE" --> Core
    Core -- "Join" --> Reference
    Reference -- "Freshness Check" --> Alerts
    Core -- "Compute Rolling 12 Mo" --> Analytics
    Analytics --> Alerts

    %% Styling
    style SourceWarehouse fill:transparent,stroke:#BBDEFB
    style Staging stroke:#C8E6C9
    style GCS stroke:#E1BEE7
    style Reference stroke:#C8E6C9
    style Composer fill:#FFD600
    style Analytics fill:#C8E6C9
    style Alerts stroke:#D50000
    style Core stroke:#C8E6C9
    ```

