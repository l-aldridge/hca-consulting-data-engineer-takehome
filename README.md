# hca-consulting-data-engineer-takehome

## Technologies Used

- **Google Cloud Platform (GCP)** – Cloud-native infrastructure supporting scalable analytics and data processing.
- **Google Cloud Storage (GCS)** – Storage layer for raw CSV source files, accessed via schema-on-read external tables.
- **BigQuery** – Primary analytical data warehouse used for ingestion, transformation, and analytics, including external tables, native fact and dimension tables, and scripted SQL for parameterized analytics.
- **GoogleSQL (BigQuery SQL dialect)** – Used for all DDL, DML, transformations, and analytics logic, including relational modeling, deterministic surrogate key generation, and complex aggregations.
- **Layered Data Architecture** – Raw → Core → Analytics design to separate ingestion, canonical modeling, and consumption-ready outputs.
- **Design-Only Orchestration & Testing** – Idempotent SQL patterns and assertion-based data quality checks designed to integrate with GCP-native schedulers (e.g., Cloud Composer).

## Part 1 – Database Schema

### Assumptions

The following assumptions were made in order to design a relational schema consistent with the provided datasets and the scope of the take-home exercise:

- **`patient_id` is treated as equivalent to an encounter identifier.**  
  Each `patient_id` represents a single hospital encounter (admission/discharge)

- **The patient dataset contains exactly one row per encounter.**  
  No duplicate `patient_id` values are expected in the patient dataset.

- **Diagnosis and procedure datasets reference encounters only via `patient_id`.**  
  These datasets do not include additional visit-level identifiers (ex. admission timestamps, visit numbers, etc).
  
- **All diagnoses and procedures are assumed to belong to the same encounter represented by `patient_id`.**  
  Diagnosis and procedure tables are modeled as child entities of the patient record.

- **`reference_year` is hardcoded to 2025 in dim_appendix_codes and in the corresponding joins.**  
  The sample dataset provided is from 2025. The expectation is that the codes will update once a year on January 1st, will be appended to dim_appendix_codes with the corresponding year.


  ### Entity Relationship Diagram (ERD)

```mermaid
erDiagram
    direction TB

    %% ------------ RAW EXTERNAL LAYER -----------
    patient_external {
        string patient_id
        string drg
        string birth_date
        string admission_date
        string discharge_date
        string admission_type
    }
    patient_procedure_external {
        string patient_id
        string procedure_code
    }
    patient_diagnosis_external {
        string patient_id
        string diag_code
        int diag_rank_num
        string present_on_admission_ind
    }
    appendix_a_external {
        string code
        string code_description
        string code_type
        string identifier
    }
    appendix_e_external {
        string code
        string code_description
        string code_type
        string identifier
    }
    appendix_f_external {
        string code
        string code_description
        string code_type
        string identifier
    }
    appendix_o_external {
        string code
        string code_description
        string code_type
        string identifier
    }

    %% ------------- REFERENCE LAYER -------------
    dim_appendix_code {
        string code_sk PK
        string code
        string code_description
        string code_type
        string identifier
        int reference_year
    }
    dim_date {
        int date_sk PK
        date full_date
        int year
        int month
        string year_month
        date month_start_date
    }


    %% ------------------ CORE LAYER -----------------
    patient {
        int patient_id PK
        string drg
        date birth_date
        date admission_date
        date discharge_date
        string admission_type
    }
    patient_procedure {
        int patient_id PK, FK
        string procedure_code PK
    }
    patient_diagnosis {
        int patient_id PK, FK
        string diag_code PK
        int diag_rank_num
        string present_on_admission_ind
    }

    %% ------------------ CORE FACTS -----------------
    fct_discharge {
        int patient_id PK, FK
        date birth_date
        int birth_date_sk FK
        int age_at_admission
        int is_adult
        date admission_date
        int admission_date_sk FK
        string admission_type
        int is_elective
        date discharge_date
        int discharge_date_sk FK
        string drg
        string drg_code_sk FK
    }
    fct_diagnosis {
        int patient_id PK, FK
        string diag_code PK
        string diag_code_sk FK
        int diag_rank_num
        string diag_type
        int is_principal_diag
        string present_on_admission_ind
        int is_present_on_admission
    }
    fct_procedure {
        int patient_id PK, FK
        string procedure_code PK
        string procedure_code_sk FK
    }

    %% ----------------- ANALYTICS SUMMARY -----------------
    discharge_summary {
        int patient_id PK, FK
        date birth_date
        int birth_date_sk FK
        int age_at_admission
        int is_adult
        date admission_date
        int admission_date_sk FK
        string admission_type
        int is_elective
        date discharge_date
        int discharge_date_sk FK
        string drg
        string drg_code_sk FK
        string drg_identifier
        struct principal_diagnosis
        array secondary_diagnoses
        array procedures
    }

    %% ------------ RELATIONSHIPS --------------

    %% Raw external to staging layers
    patient_external ||--o| patient : "ETL"
    patient_procedure_external ||--o| patient_procedure : "ETL"
    patient_diagnosis_external ||--o| patient_diagnosis : "ETL"
    appendix_a_external ||--o| dim_appendix_code : "ETL"
    appendix_e_external ||--o| dim_appendix_code : "ETL"
    appendix_f_external ||--o| dim_appendix_code : "ETL"
    appendix_o_external ||--o| dim_appendix_code : "ETL"

    %% Reference FKs
    fct_discharge }o--|| dim_date : "birth_date_sk, admission_date_sk, discharge_date_sk"
    fct_discharge }o--|| dim_appendix_code : "drg_code_sk"
    fct_diagnosis }o--|| dim_appendix_code : "diag_code_sk"
    fct_procedure }o--|| dim_appendix_code : "procedure_code_sk"
    discharge_summary }o--|| dim_date : "birth_date_sk, admission_date_sk, discharge_date_sk"
    discharge_summary }o--|| dim_appendix_code : "drg_code_sk"

    %% Core to fact relationships
    patient ||--o| fct_discharge : ""
    patient ||--o| fct_diagnosis : ""
    patient ||--o| fct_procedure : ""
    patient ||--o| patient_procedure : ""
    patient ||--o| patient_diagnosis : ""

    %% Facts to summary
    fct_discharge ||--o| discharge_summary : ""

    %% ------------- COLOR CLASSES --------------
    classDef raw fill:#D1B3FF,stroke:#8041D9,stroke-width:2px
    classDef ref fill:#B3FFC6,stroke:#23A769,stroke-width:2px
    classDef core fill:#B3DAFF,stroke:#1F78B4,stroke-width:2px
    classDef fact fill:#FFD580,stroke:#FFA600,stroke-width:2px
    classDef summary fill:#FFEAAA,stroke:#C09800,stroke-width:2px

    class patient_external,patient_procedure_external,patient_diagnosis_external,appendix_a_external,appendix_e_external,appendix_f_external,appendix_o_external raw
    class dim_appendix_code,dim_date ref
    class patient,patient_procedure,patient_diagnosis core
    class fct_discharge,fct_diagnosis,fct_procedure fact
    class discharge_summary summary
```    
  
## Part 2 – PSI-13 monthly output (2025)

### Description

The analytics.psi13_monthly_metrics table is built via a scripted BigQuery SQL file using a parameterized list of PSI-13 sepsis diagnosis codes and produces month-level numerator, denominator, and rate metrics.

## Part 3 - Design Only Nightly ETL for a rolling 12-month window

Reference `etl_design.md`