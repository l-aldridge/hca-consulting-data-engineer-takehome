-- Run order: 02 of 03
CREATE TABLE IF NOT EXISTS `hca-takehome.core.patient` (
    patient_id INT NOT NULL,
    drg STRING,
    birth_date DATE,
    admission_date DATE,
    discharge_date DATE,
    admission_type STRING,
    PRIMARY KEY (patient_id) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS `hca-takehome.core.patient_procedure` (
    patient_id INT NOT NULL,
    procedure_code STRING,
    PRIMARY KEY (patient_id, procedure_code) NOT ENFORCED,
    FOREIGN KEY (patient_id) REFERENCES `hca-takehome.core.patient` (patient_id) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS `hca-takehome.core.patient_diagnosis` (
    patient_id INT NOT NULL,
    diag_code STRING,
    diag_rank_num INT,
    present_on_admission_ind STRING,
    PRIMARY KEY (patient_id, diag_code) NOT ENFORCED,
    FOREIGN KEY (patient_id) REFERENCES `hca-takehome.core.patient` (patient_id) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS `hca-takehome.core.fct_discharge` (
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
    drg STRING NOT NULL,
    drg_code_sk STRING NOT NULL,
    PRIMARY KEY (patient_id) NOT ENFORCED,
    FOREIGN KEY (patient_id) REFERENCES `hca-takehome.core.patient` (patient_id) NOT ENFORCED,
    FOREIGN KEY (birth_date_sk) REFERENCES `hca-takehome.reference.dim_date` (date_sk) NOT ENFORCED,
    FOREIGN KEY (admission_date_sk) REFERENCES `hca-takehome.reference.dim_date` (date_sk) NOT ENFORCED,
    FOREIGN KEY (discharge_date_sk) REFERENCES `hca-takehome.reference.dim_date` (date_sk) NOT ENFORCED,
    FOREIGN KEY (drg_code_sk) REFERENCES `hca-takehome.reference.dim_appendix_code` (code_sk) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS `hca-takehome.core.fct_diagnosis` (
    patient_id INT NOT NULL,
    diag_code STRING NOT NULL,
    diag_code_sk STRING NOT NULL,
    diag_rank_num INT NOT NULL,
    diag_type STRING,
    is_principal_diag INT NOT NULL,
    present_on_admission_ind STRING,
    is_present_on_admission INT NOT NULL,
    PRIMARY KEY (patient_id, diag_code) NOT ENFORCED,
    FOREIGN KEY (patient_id) REFERENCES `hca-takehome.core.patient` (patient_id) NOT ENFORCED,
    FOREIGN KEY (diag_code_sk) REFERENCES `hca-takehome.reference.dim_appendix_code` (code_sk) NOT ENFORCED
);
CREATE TABLE IF NOT EXISTS `hca-takehome.core.fct_procedure` (
    patient_id INT NOT NULL,
    procedure_code STRING NOT NULL,
    procedure_code_sk STRING NOT NULL,
    PRIMARY KEY (patient_id, procedure_code) NOT ENFORCED,
    FOREIGN KEY (patient_id) REFERENCES `hca-takehome.core.patient` (patient_id) NOT ENFORCED,
    FOREIGN KEY (procedure_code_sk) REFERENCES `hca-takehome.reference.dim_appendix_code` (code_sk) NOT ENFORCED
);

-- remove any existing data to prevent duplicates
TRUNCATE TABLE `hca-takehome.core.patient`;
TRUNCATE TABLE `hca-takehome.core.patient_procedure`;
TRUNCATE TABLE `hca-takehome.core.patient_diagnosis`;
TRUNCATE TABLE `hca-takehome.core.fct_discharge`;
TRUNCATE TABLE `hca-takehome.core.fct_diagnosis`;
TRUNCATE TABLE `hca-takehome.core.fct_procedure`;

INSERT INTO `hca-takehome.core.patient` (
        patient_id,
        drg,
        birth_date,
        admission_date,
        discharge_date,
        admission_type
    )
SELECT CAST(patient_id AS INT) AS patient_id,
    drg,
    SAFE.PARSE_DATE('%Y-%m-%d', birth_date) AS birth_date,
    SAFE.PARSE_DATE('%Y-%m-%d', admission_date) AS admission_date,
    SAFE.PARSE_DATE('%Y-%m-%d', discharge_date) AS discharge_date,
    admission_type
FROM `hca-takehome.raw_ext.patient_external`;

INSERT INTO `hca-takehome.core.patient_procedure` (patient_id, procedure_code)
SELECT CAST(patient_id AS INT) AS patient_id,
    procedure_code
FROM `hca-takehome.raw_ext.patient_procedure_external`;

INSERT INTO `hca-takehome.core.patient_diagnosis` (
        patient_id,
        diag_code,
        diag_rank_num,
        present_on_admission_ind
    )
SELECT CAST(patient_id AS INT) AS   patient_id,
    diag_code,
    diag_rank_num,
    present_on_admission_ind
FROM `hca-takehome.raw_ext.patient_diagnosis_external`;

--for the purposes of the takehome the JOIN with dim_appendix_code.reference year
--is hardcoded to 2025. In a production scenario, this would be dynamic based on year of discharge_date.

INSERT INTO `hca-takehome.core.fct_discharge` (
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
        drg_code_sk
    )
SELECT p.patient_id,
    COALESCE(birth_date, DATE '1900-01-01') AS birth_date,
    COALESCE(bd.date_sk, -1) AS birth_date_sk,
    DATE_DIFF(p.admission_date, p.birth_date, YEAR) AS age_at_admission,
    CASE
        WHEN DATE_DIFF(p.admission_date, p.birth_date, YEAR) >= 18 THEN 1
        ELSE 0
    END AS is_adult,
    COALESCE(p.admission_date, DATE '1900-01-01') AS admission_date,
    COALESCE(ad.date_sk, -1) AS admission_date_sk,
    COALESCE(p.admission_type, 'UNKNOWN') AS admission_type,
    CASE
        WHEN LOWER(p.admission_type) = 'elective' THEN 1
        ELSE 0
    END AS is_elective,
    COALESCE(p.discharge_date, DATE '1900-01-01') AS discharge_date,
    COALESCE(dd.date_sk, -1) AS discharge_date_sk,
    COALESCE(drg, 'UNKNOWN') AS drg,
    COALESCE(dc.code_sk, 'UNKNOWN') AS drg_code_sk
FROM `hca-takehome.core.patient` AS p
 LEFT JOIN `hca-takehome.reference.dim_date` AS bd
    ON COALESCE(birth_date, DATE '1900-01-01') = bd.full_date
 LEFT JOIN `hca-takehome.reference.dim_date` AS ad
    ON COALESCE(admission_date, DATE '1900-01-01') = ad.full_date
 LEFT JOIN `hca-takehome.reference.dim_date` AS dd
    ON COALESCE(discharge_date, DATE '1900-01-01') = dd.full_date
 LEFT JOIN `hca-takehome.reference.dim_appendix_code` AS dc
    ON COALESCE(p.drg, 'UNKNOWN') = dc.code
    AND dc.code_type = 'DRG' 
    AND dc.reference_year = 2025;

INSERT INTO `hca-takehome.core.fct_diagnosis` (
        patient_id,
        diag_code,
        diag_code_sk,
        diag_rank_num,
        diag_type,
        is_principal_diag,
        present_on_admission_ind,
        is_present_on_admission
    )
SELECT pd.patient_id,
    COALESCE(pd.diag_code, 'UNKNOWN') AS diag_code,
    COALESCE(dc.code_sk, 'UNKNOWN') AS diag_code_sk,
    pd.diag_rank_num,
    CASE
        WHEN pd.diag_rank_num = 1 THEN 'Principal'
        WHEN pd.diag_rank_num>1 THEN 'Secondary'
        ELSE 'UNKNOWN'
    END AS diag_type,
    CASE
        WHEN pd.diag_rank_num = 1 THEN 1
        ELSE 0
    END AS is_principal_diag,
    COALESCE(pd.present_on_admission_ind, 'N') AS present_on_admission_ind,
    CASE
        WHEN pd.present_on_admission_ind = 'Y' THEN 1
        ELSE 0
    END AS is_present_on_admission
FROM `hca-takehome.core.patient_diagnosis` pd
LEFT JOIN `hca-takehome.core.patient` p
    ON pd.patient_id = p.patient_id
LEFT JOIN `hca-takehome.reference.dim_appendix_code` AS dc
    ON COALESCE(pd.diag_code, 'UNKNOWN') = dc.code
    AND dc.code_type = 'Diagnosis'
    AND dc.reference_year = 2025;

INSERT INTO `hca-takehome.core.fct_procedure` (
    patient_id, 
    procedure_code,
    procedure_code_sk)
SELECT pp.patient_id,
    pp.procedure_code,
    COALESCE(pc.code_sk, 'UNKNOWN') AS procedure_code_sk
FROM `hca-takehome.core.patient_procedure` pp
LEFT JOIN `hca-takehome.core.patient` p
    ON pp.patient_id = p.patient_id
LEFT JOIN `hca-takehome.reference.dim_appendix_code` AS pc
    ON COALESCE(pp.procedure_code, 'UNKNOWN') = pc.code
    AND pc.code_type = 'Procedure'
    AND pc.reference_year = 2025;