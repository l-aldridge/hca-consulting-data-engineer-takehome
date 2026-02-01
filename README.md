# hca-consulting-data-engineer-takehome
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

  ### Entity Relationship Diagram (ERD)


```mermaid
erDiagram
  direction TB

  %% -----RAW EXTERNAL LAYER----
  raw_patient_external["RAW_PATIENT_EXTERNAL"] {
    string patient_id
    string drg
    string birth_date
    string admission_date
    string discharge_date
    string admission_type
  }
  raw_patient_diagnosis_external["RAW_PATIENT_DIAGNOSIS_EXTERNAL"] {
    string patient_id
    string diag_code
    int diag_rank_num
    string present_on_admission_ind
  }
  raw_patient_procedure_external["RAW_PATIENT_PROCEDURE_EXTERNAL"] {
    string patient_id
    string procedure_code
  }
  raw_appendix_external["RAW_APPENDIX_EXTERNAL"] {
    string code
    string code_description
    string code_type
    string identifier
  }

  %% -----REFERENCE LAYER----
  dim_appendix_code["DIM_APPENDIX_CODE"] {
    string code PK
    string code_description
    string code_type
    string identifier PK
  }

  %% -----CORE LAYER----
  patient["PATIENT"] {
    int patient_id PK
    string drg
    date birth_date
    date admission_date
    date discharge_date
    string admission_type
  }
  patient_diagnosis["PATIENT_DIAGNOSIS"] {
    int patient_id PK, FK
    string diag_code PK
    int diag_rank_num
    string present_on_admission_ind
  }
  patient_procedure["PATIENT_PROCEDURE"] {
    int patient_id PK, FK
    string procedure_code PK
  }

  %% -----ANALYTICS LAYER----
  fct_discharge["FCT_DISCHARGE"] {
    int patient_id PK, FK
    date birth_date
    int age_at_admission
    int is_adult
    date admission_date
    string admission_type
    int is_elective
    date discharge_date
    int days_admitted
    string drg
  }
  fct_diagnosis["FCT_DIAGNOSIS"] {
    int patient_id PK, FK
    string diag_code PK
    int diag_rank_num
    string diag_type
    int is_principal_diag
    string present_on_admission_ind
    int is_present_on_admission
  }
  fct_procedure["FCT_PROCEDURE"] {
    int patient_id PK, FK
    string procedure_code PK
  }
  discharge_summary["DISCHARGE_SUMMARY"] {
    int patient_id PK, FK
    date birth_date
    int age_at_admission
    int is_adult
    date admission_date
    string admission_type
    int is_elective
    date discharge_date
    int days_admitted
    string drg
    string drg_identifier
    string principal_diag_code
    string principal_diag_identifier
    int principal_diag_present_on_admission
    string[] secondary_diag_present_on_admission_codes
    string[] secondary_diag_present_on_admission_identifiers
    string[] secondary_diag_not_present_on_admission_codes
    string[] secondary_diag_not_present_on_admission_identifiers
    string[] procedure_codes
    string[] procedure_identifiers
  }

  %% ---RAW TO CORE---
  raw_patient_external ||--o| patient : "ETL"
  raw_patient_diagnosis_external ||--o| patient_diagnosis : "ETL"
  raw_patient_procedure_external ||--o| patient_procedure : "ETL"
  raw_appendix_external ||--o| dim_appendix_code : "ETL"

  %% ---REFERENCE/CODE RELATIONSHIPS---
  patient_diagnosis }o..|| dim_appendix_code : "diag_code to code"
  patient_procedure }o..|| dim_appendix_code : "procedure_code to code"
  fct_diagnosis }o..|| dim_appendix_code : "diag_code to code"
  fct_procedure }o..|| dim_appendix_code : "procedure_code to code"

  %% ---CORE LAYER RELATIONSHIPS---
  patient ||--o| patient_diagnosis : ""
  patient ||--o| patient_procedure : ""

  %% ---ANALYTICS LAYER RELATIONSHIPS---
  patient ||--o| fct_discharge : ""
  patient ||--o| fct_diagnosis : ""
  patient ||--o| fct_procedure : ""
  fct_discharge ||--o| fct_diagnosis : "by patient_id"
  fct_discharge ||--o| fct_procedure : "by patient_id"
  fct_discharge ||--|| discharge_summary : "by patient_id"

  %% ---DISCHARGE SUMMARY CODE RELATIONSHIPS---
  discharge_summary }o..|| dim_appendix_code : "principal_diag_code, drg (via identifier/joins)"
  discharge_summary }o..|| dim_appendix_code : "procedure_codes (array joins)"

  %% Color Classes
  classDef raw fill:#D1B3FF,stroke:#8041D9,stroke-width:2px
  classDef core fill:#B3DAFF,stroke:#1F78B4,stroke-width:2px
  classDef fact fill:#FFD580,stroke:#FFA600,stroke-width:2px
  classDef dim fill:#B3FFC6,stroke:#23A769,stroke-width:2px
  classDef summary fill:#FFEAAA,stroke:#C09800,stroke-width:2px

  class raw_patient_external,raw_patient_diagnosis_external,raw_patient_procedure_external,raw_appendix_external raw
  class patient,patient_diagnosis,patient_procedure core
  class fct_discharge,fct_diagnosis,fct_procedure fact
  class dim_appendix_code dim
  class discharge_summary summary
```
