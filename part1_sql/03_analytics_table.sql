-- Run order: 03 of 03
CREATE TABLE IF NOT EXISTS `hca-takehome`.`analytics`.`fct_discharge` (
    patient_id INT NOT NULL,
    birth_date DATE NOT NULL,
    age_at_admission INT NOT NULL,
    is_adult INT NOT NULL,
    admission_date DATE NOT NULL,
    admission_type STRING NOT NULL,
    is_elective INT NOT NULL,
    discharge_date DATE NOT NULL,
    days_admitted INT NOT NULL,
    drg STRING,
    PRIMARY KEY (patient_id) NOT ENFORCED,
    FOREIGN KEY (patient_id) REFERENCES `hca-takehome.core.patient` (patient_id) NOT ENFORCED
);
CREATE TABLE IF NOT EXISTS `hca-takehome`.`analytics`.`fct_diagnosis` (
    patient_id INT NOT NULL,
    diag_code STRING NOT NULL,
    diag_rank_num INT NOT NULL,
    diag_type STRING,
    is_principal_diag INT NOT NULL,
    present_on_admission_ind STRING,
    is_present_on_admission INT NOT NULL,
    PRIMARY KEY (patient_id, diag_code) NOT ENFORCED,
    FOREIGN KEY (patient_id) REFERENCES `hca-takehome.core.patient` (patient_id) NOT ENFORCED
);
CREATE TABLE IF NOT EXISTS `hca-takehome`.`analytics`.`fct_procedure` (
    patient_id INT NOT NULL,
    procedure_code STRING NOT NULL,
    PRIMARY KEY (patient_id, procedure_code) NOT ENFORCED,
    FOREIGN KEY (patient_id) REFERENCES `hca-takehome.core.patient` (patient_id) NOT ENFORCED
);
CREATE TABLE IF NOT EXISTS `hca-takehome`.`analytics`.`discharge_summary` (
    patient_id INT NOT NULL,
    birth_date DATE NOT NULL,
    age_at_admission INT NOT NULL,
    is_adult INT NOT NULL,
    admission_date DATE NOT NULL,
    admission_type STRING NOT NULL,
    is_elective INT NOT NULL,
    discharge_date DATE NOT NULL,
    days_admitted INT NOT NULL,
    drg STRING,
    drg_identifier STRING,
    principal_diag_code STRING,
    principal_diag_identifier STRING,
    principal_diag_present_on_admission INT,
    secondary_diag_present_on_admission_codes ARRAY < STRING >,
    secondary_diag_present_on_admission_identifiers ARRAY < STRING >,
    secondary_diag_not_present_on_admission_codes ARRAY < STRING >,
    secondary_diag_not_present_on_admission_identifiers ARRAY < STRING >,
    procedure_codes ARRAY < STRING >,
    procedure_identifiers ARRAY < STRING >,
    PRIMARY KEY (patient_id) NOT ENFORCED,
    FOREIGN KEY (patient_id) REFERENCES `hca-takehome.core.patient` (patient_id) NOT ENFORCED
);
INSERT INTO `hca-takehome`.`analytics`.`fct_discharge` (
        patient_id,
        birth_date,
        age_at_admission,
        is_adult,
        admission_date,
        admission_type,
        is_elective,
        discharge_date,
        days_admitted,
        drg
    )
SELECT patient_id,
    birth_date,
    DATE_DIFF(admission_date, birth_date, YEAR) AS age_at_admission,
    CASE
        WHEN DATE_DIFF(admission_date, birth_date, YEAR) >= 18 THEN 1
        ELSE 0
    END AS is_adult,
    admission_date,
    admission_type,
    CASE
        WHEN LOWER(admission_type) = 'elective' THEN 1
        ELSE 0
    END AS is_elective,
    discharge_date,
    DATE_DIFF(discharge_date, admission_date, DAY) AS days_admitted,
    drg
FROM `hca-takehome`.`core`.`patient`;
INSERT INTO `hca-takehome`.`analytics`.`fct_diagnosis` (
        patient_id,
        diag_code,
        diag_rank_num,
        diag_type,
        is_principal_diag,
        present_on_admission_ind,
        is_present_on_admission
    )
SELECT patient_id,
    diag_code,
    diag_rank_num,
    CASE
        WHEN diag_rank_num = 1 THEN 'Principal'
        ELSE 'Secondary'
    END AS diag_type,
    CASE
        WHEN diag_rank_num = 1 THEN 1
        ELSE 0
    END AS is_principal_diag,
    present_on_admission_ind,
    CASE
        WHEN present_on_admission_ind = 'Y' THEN 1
        ELSE 0
    END AS is_present_on_admission
FROM `hca-takehome`.`core`.`patient_diagnosis`;
INSERT INTO `hca-takehome`.`analytics`.`fct_procedure` (patient_id, procedure_code)
SELECT patient_id,
    procedure_code
FROM `hca-takehome`.`core`.`patient_procedure`;
INSERT INTO `hca-takehome`.`analytics`.`discharge_summary` (
        patient_id,
        birth_date,
        age_at_admission,
        is_adult,
        admission_date,
        admission_type,
        is_elective,
        discharge_date,
        days_admitted,
        drg,
        drg_identifier,
        principal_diag_code,
        principal_diag_identifier,
        principal_diag_present_on_admission,
        secondary_diag_present_on_admission_codes,
        secondary_diag_present_on_admission_identifiers,
        secondary_diag_not_present_on_admission_codes,
        secondary_diag_not_present_on_admission_identifiers,
        procedure_codes,
        procedure_identifiers
    ) WITH diag AS (
        SELECT patient_id,
            MAX(IF(dg.is_principal_diag = 1, dg.diag_code, NULL)) AS principal_diag_code,
            MAX(
                IF(dg.is_principal_diag = 1, dgi.identifier, NULL)
            ) AS principal_diag_identifier,
            MAX(
                IF(
                    dg.is_principal_diag = 1
                    AND dg.is_present_on_admission = 1,
                    1,
                    0
                )
            ) AS principal_diag_present_on_admission,
            ARRAY_AGG(
                IF(
                    dg.is_principal_diag = 0
                    AND dg.is_present_on_admission = 1,
                    dg.diag_code,
                    NULL
                ) IGNORE NULLS
            ) AS secondary_diag_present_on_admission_codes,
            ARRAY_AGG(
                DISTINCT IF(
                    dg.is_principal_diag = 0
                    AND dg.is_present_on_admission = 1,
                    dgi.identifier,
                    NULL
                ) IGNORE NULLS
            ) AS secondary_diag_present_on_admission_identifiers,
            ARRAY_AGG(
                IF(
                    dg.is_principal_diag = 0
                    AND dg.is_present_on_admission = 0,
                    dg.diag_code,
                    NULL
                ) IGNORE NULLS
            ) AS secondary_diag_not_present_on_admission_codes,
            ARRAY_AGG(
                DISTINCT IF(
                    dg.is_principal_diag = 0
                    AND dg.is_present_on_admission = 0,
                    dgi.identifier,
                    NULL
                ) IGNORE NULLS
            ) AS secondary_diag_not_present_on_admission_identifiers
        FROM `hca-takehome.analytics.fct_diagnosis` dg
            LEFT JOIN `hca-takehome.reference.dim_appendix_code` dgi ON dg.diag_code = dgi.code
            AND dgi.code_type = 'Diagnosis'
        GROUP BY dg.patient_id
    ),
    proc AS (
        SELECT fp.patient_id,
            ARRAY_AGG(fp.procedure_code IGNORE NULLS) AS procedure_codes,
            ARRAY_AGG(pci.identifier IGNORE NULLS) AS procedure_identifiers
        FROM `hca-takehome.analytics.fct_procedure` fp
            LEFT JOIN `hca-takehome.reference.dim_appendix_code` pci ON fp.procedure_code = pci.code
            AND pci.code_type = 'Procedure'
        GROUP BY fp.patient_id
    )
SELECT d.patient_id,
    d.birth_date,
    d.age_at_admission,
    d.is_adult,
    d.admission_date,
    d.admission_type,
    d.is_elective,
    d.discharge_date,
    d.days_admitted,
    d.drg,
    ac.identifier AS drg_identifier,
    diag.principal_diag_code,
    diag.principal_diag_identifier,
    diag.principal_diag_present_on_admission,
    diag.secondary_diag_present_on_admission_codes,
    diag.secondary_diag_present_on_admission_identifiers,
    diag.secondary_diag_not_present_on_admission_codes,
    diag.secondary_diag_not_present_on_admission_identifiers,
    proc.procedure_codes,
    proc.procedure_identifiers
FROM `hca-takehome.analytics.fct_discharge` d
    LEFT JOIN `hca-takehome.reference.dim_appendix_code` ac ON d.drg = ac.code
    AND ac.code_type = 'DRG'
    LEFT JOIN diag ON d.patient_id = diag.patient_id
    LEFT JOIN proc ON d.patient_id = proc.patient_id;