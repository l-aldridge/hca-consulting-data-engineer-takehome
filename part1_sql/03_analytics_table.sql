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
        drg_identifier STRING,
        principal_diagnosis STRUCT < code_sk STRING,
        code STRING,
        identifier STRING,
        is_present_on_admission INT >,
        secondary_diagnoses ARRAY < STRUCT < code_sk STRING,
        code STRING,
        identifier STRING,
        is_present_on_admission INT >>,
        procedures ARRAY < STRUCT < code_sk STRING,
        code STRING,
        identifier STRING >>,
        PRIMARY KEY (patient_id) NOT ENFORCED,
        FOREIGN KEY (patient_id) REFERENCES `hca-takehome.core.fct_discharge` (patient_id) NOT ENFORCED,
        FOREIGN KEY (birth_date_sk) REFERENCES `hca-takehome.reference.dim_date` (date_sk) NOT ENFORCED,
        FOREIGN KEY (admission_date_sk) REFERENCES `hca-takehome.reference.dim_date` (date_sk) NOT ENFORCED,
        FOREIGN KEY (discharge_date_sk) REFERENCES `hca-takehome.reference.dim_date` (date_sk) NOT ENFORCED,
        FOREIGN KEY (drg_code_sk) REFERENCES `hca-takehome.reference.dim_appendix_code` (code_sk) NOT ENFORCED
    ) PARTITION BY DATE_TRUNC(admission_date, MONTH);
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
        COALESCE(ac.code_sk, 'Unknown') AS drg_code_sk,
        ac.identifier AS drg_identifier,
        principal_diagnosis,
        secondary_diagnoses,
        procedures
    ) WITH diag AS (
        SELECT dg.patient_id,
            (
                ARRAY_AGG(
                    IF(
                        dg.is_principal_diag = 1,
                        STRUCT(
                            dgi.code_sk AS code_sk,
                            dg.diag_code AS code,
                            dgi.identifier AS identifier,
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
                        dgi.code_sk AS code_sk,
                        dg.diag_code AS code,
                        dgi.identifier AS identifier,
                        dg.is_present_on_admission AS is_present_on_admission
                    ),
                    NULL
                ) IGNORE NULLS
                ORDER BY dg.diag_rank_num
            ) AS secondary_diagnoses
        FROM `hca-takehome.core.fct_diagnosis` dg
            LEFT JOIN `hca-takehome.reference.dim_appendix_code` dgi 
            ON dg.diag_code = dgi.code
            AND dgi.code_type = 'Diagnosis'
        GROUP BY dg.patient_id
    ),
    proc AS (
        SELECT fp.patient_id,
            ARRAY_AGG(
                STRUCT(
                    pci.code_sk AS code_sk,
                    fp.procedure_code AS code,
                    pci.identifier AS identifier
                )
                ORDER BY fp.procedure_code
            ) AS procedures
        FROM `hca-takehome.core.fct_procedure` fp
            LEFT JOIN `hca-takehome.reference.dim_appendix_code` pci 
            ON fp.procedure_code = pci.code
            AND pci.code_type = 'Procedure'
        GROUP BY fp.patient_id
    )
SELECT d.patient_id,
    d.birth_date,
    bd.date_sk AS birth_date_sk,
    d.age_at_admission,
    d.is_adult,
    d.admission_date,
    ad.date_sk AS admission_date_sk,
    d.admission_type,
    d.is_elective,
    d.discharge_date,
    ds.date_sk AS discharge_date_sk,
    d.drg,
    ac.code_sk AS drg_code_sk,
    ac.identifier AS drg_identifier,
    diag.principal_diagnosis,
    diag.secondary_diagnoses,
    proc.procedures
FROM `hca-takehome.core.fct_discharge` d
    LEFT JOIN `hca-takehome.reference.dim_date` bd 
    ON d.birth_date = bd.full_date
    LEFT JOIN `hca-takehome.reference.dim_date` ad 
    ON d.admission_date = ad.full_date
    LEFT JOIN `hca-takehome.reference.dim_date` ds 
    ON d.discharge_date = ds.full_date
    LEFT JOIN `hca-takehome.reference.dim_appendix_code` ac 
    ON d.drg = ac.code
    AND ac.code_type = 'DRG'
    LEFT JOIN diag ON d.patient_id = diag.patient_id
    LEFT JOIN proc ON d.patient_id = proc.patient_id; 