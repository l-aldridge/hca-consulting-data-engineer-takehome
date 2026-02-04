-- Run order: 02 of 03
CREATE TABLE IF NOT EXISTS `hca-takehome`.`core`.`patient` (
    patient_id INT NOT NULL,
    drg STRING,
    birth_date DATE NOT NULL,
    admission_date DATE NOT NULL,
    discharge_date DATE NOT NULL,
    admission_type STRING NOT NULL,
    PRIMARY KEY (patient_id) NOT ENFORCED
);
CREATE TABLE IF NOT EXISTS `hca-takehome`.`core`.`patient_procedure` (
    patient_id INT NOT NULL,
    procedure_code STRING NOT NULL,
    PRIMARY KEY (patient_id, procedure_code) NOT ENFORCED,
    FOREIGN KEY (patient_id) REFERENCES `hca-takehome.core.patient` (patient_id) NOT ENFORCED
);
CREATE TABLE IF NOT EXISTS `hca-takehome`.`core`.`patient_diagnosis` (
    patient_id INT NOT NULL,
    diag_code STRING NOT NULL,
    diag_rank_num INT NOT NULL,
    present_on_admission_ind STRING,
    PRIMARY KEY (patient_id, diag_code) NOT ENFORCED,
    FOREIGN KEY (patient_id) REFERENCES `hca-takehome.core.patient` (patient_id) NOT ENFORCED
);
INSERT INTO `hca-takehome`.`core`.`patient` (
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
FROM `hca-takehome`.`raw_ext`.`patient_external`;
INSERT INTO `hca-takehome`.`core`.`patient_procedure` (patient_id, procedure_code)
SELECT CAST(patient_id AS INT) AS patient_id,
    procedure_code
FROM `hca-takehome`.`raw_ext`.`patient_procedure_external`;
INSERT INTO `hca-takehome`.`core`.`patient_diagnosis` (
        patient_id,
        diag_code,
        diag_rank_num,
        present_on_admission_ind
    )
SELECT CAST(patient_id AS INT) AS patient_id,
    diag_code,
    diag_rank_num,
    present_on_admission_ind
FROM `hca-takehome`.`raw_ext`.`patient_diagnosis_external`;