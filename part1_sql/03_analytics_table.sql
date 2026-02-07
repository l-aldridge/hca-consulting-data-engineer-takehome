-- Run order: 03 of 03 
CREATE OR REPLACE TABLE `hca-takehome.analytics.discharge_summary` (
        patient_id INT NOT NULL,
        birth_date DATE NOT NULL,
        birth_date_sk INT NOT NULL,
        age_at_admission INT NOT NULL,
        is_adult INT NOT NULL,
        admission_date DATE NOT NULL,
        admission_date_sk INT NOT NULL,
        admission_type STRING NOT NULL,
        is_elective INT NOT NULL,
        discharge_date DATE NOT NULL,
        discharge_date_sk INT NOT NULL,
        drg STRING,
        drg_code_sk STRING NOT NULL,
        drg_identifiers ARRAY < STRING >,
        principal_diagnosis STRUCT < code_sk STRING,
        code STRING,
        identifiers ARRAY < STRING >,
        is_present_on_admission INT >,
        secondary_diagnoses ARRAY < STRUCT < code_sk STRING,
        code STRING,
        identifiers ARRAY < STRING >,
        is_present_on_admission INT >>,
        procedures ARRAY < STRUCT < code_sk STRING,
        code STRING,
        identifiers ARRAY < STRING > >>,
        PRIMARY KEY (patient_id) NOT ENFORCED,
        FOREIGN KEY (patient_id) REFERENCES `hca-takehome.core.fct_discharge` (patient_id) NOT ENFORCED,
        FOREIGN KEY (birth_date_sk) REFERENCES `hca-takehome.reference.dim_date` (date_sk) NOT ENFORCED,
        FOREIGN KEY (admission_date_sk) REFERENCES `hca-takehome.reference.dim_date` (date_sk) NOT ENFORCED,
        FOREIGN KEY (discharge_date_sk) REFERENCES `hca-takehome.reference.dim_date` (date_sk) NOT ENFORCED,
        FOREIGN KEY (drg_code_sk) REFERENCES `hca-takehome.reference.dim_code` (code_sk) NOT ENFORCED
    ) PARTITION BY DATE_TRUNC(discharge_date, MONTH);

INSERT INTO `hca-takehome.analytics.discharge_summary` (
        patient_id,
        birth_date,
        birth_date_sk,
        age_at_admission,
        is_adult,
        admission_date,
        admission_date_sk,
        admission_type,
        is_elective,
        discharge_date,
        discharge_date_sk,
        drg,
        drg_code_sk,
        drg_identifiers,
        principal_diagnosis,
        secondary_diagnoses,
        procedures
    ) WITH code_identifiers AS (
        SELECT code_sk,
            code_type,
            reference_year,
            ARRAY_AGG(
                DISTINCT identifier
                ORDER BY identifier
            ) AS identifiers
        FROM `hca-takehome.reference.bridge_code_identifier`
        WHERE reference_year = 2025
        GROUP BY code_sk,
            code_type,
            reference_year
    ),
    diag AS (
        SELECT dg.patient_id,
            (
                ARRAY_AGG(
                    IF(
                        dg.is_principal_diag = 1,
                        STRUCT(
                            dc.code_sk AS code_sk,
                            dg.diag_code AS code,
                            COALESCE(ci.identifiers, []) AS identifiers,
                            dg.is_present_on_admission AS is_present_on_admission
                        ),
                        NULL
                    ) IGNORE NULLS
                    ORDER BY dg.diag_rank_num
                    LIMIT 1
                )
            ) [OFFSET(0)] AS principal_diagnosis,
            ARRAY_AGG(
                IF(
                    dg.is_principal_diag = 0,
                    STRUCT(
                        dc.code_sk AS code_sk,
                        dg.diag_code AS code,
                        COALESCE(ci.identifiers, []) AS identifiers,
                        dg.is_present_on_admission AS is_present_on_admission
                    ),
                    NULL
                ) IGNORE NULLS
                ORDER BY dg.diag_rank_num
            ) AS secondary_diagnoses
        FROM `hca-takehome.core.fct_diagnosis` dg
            LEFT JOIN `hca-takehome.reference.dim_code` dc ON dc.code = dg.diag_code
            AND dc.code_type = 'Diagnosis'
            AND dc.reference_year = 2025
            LEFT JOIN code_identifiers ci ON ci.code_sk = dc.code_sk
            AND ci.code_type = 'Diagnosis'
            AND ci.reference_year = 2025
        GROUP BY dg.patient_id
    ),
    proc AS (
        SELECT fp.patient_id,
            ARRAY_AGG(
                STRUCT(
                    pc.code_sk AS code_sk,
                    fp.procedure_code AS code,
                    COALESCE(ci.identifiers, []) AS identifiers
                )
                ORDER BY fp.procedure_code
            ) AS procedures
        FROM `hca-takehome.core.fct_procedure` fp
            LEFT JOIN `hca-takehome.reference.dim_code` pc ON pc.code = fp.procedure_code
            AND pc.code_type = 'Procedure'
            AND pc.reference_year = 2025
            LEFT JOIN code_identifiers ci ON ci.code_sk = pc.code_sk
            AND ci.code_type = 'Procedure'
            AND ci.reference_year = 2025
        GROUP BY fp.patient_id
    ),
    drg AS (
        SELECT d.patient_id,
            d.drg,
            dc.code_sk AS drg_code_sk,
            COALESCE(ci.identifiers, []) AS drg_identifiers
        FROM `hca-takehome.core.fct_discharge` d
            LEFT JOIN `hca-takehome.reference.dim_code` dc ON d.drg = dc.code
            AND dc.code_type = 'DRG'
            AND dc.reference_year = 2025
            LEFT JOIN code_identifiers ci ON ci.code_sk = dc.code_sk
            AND ci.code_type = 'DRG'
            AND ci.reference_year = 2025
    )
SELECT d.patient_id,
    d.birth_date,
    COALESCE(bd.date_sk, -1) AS birth_date_sk,
    d.age_at_admission,
    d.is_adult,
    d.admission_date,
    COALESCE(ad.date_sk, -1) AS admission_date_sk,
    d.admission_type,
    d.is_elective,
    d.discharge_date,
    ds.date_sk AS discharge_date_sk,
    d.drg,
    -- keep your previous behavior; note: if you want UNKNOWN hash fallback instead, say so
    COALESCE(drg.drg_code_sk, 'Unknown') AS drg_code_sk,
    COALESCE(drg.drg_identifiers, []) AS drg_identifiers,
    diag.principal_diagnosis,
    diag.secondary_diagnoses,
    proc.procedures
FROM `hca-takehome.core.fct_discharge` d
    LEFT JOIN `hca-takehome.reference.dim_date` bd ON d.birth_date = bd.full_date
    LEFT JOIN `hca-takehome.reference.dim_date` ad ON d.admission_date = ad.full_date
    LEFT JOIN `hca-takehome.reference.dim_date` ds ON d.discharge_date = ds.full_date
    LEFT JOIN drg ON d.patient_id = drg.patient_id
    LEFT JOIN diag ON d.patient_id = diag.patient_id
    LEFT JOIN proc ON d.patient_id = proc.patient_id;