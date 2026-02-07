-- Run order: 02 of 03
CREATE TABLE IF NOT EXISTS `hca-takehome.reference.dim_appendix_code` (
    code_sk STRING NOT NULL,
    code STRING NOT NULL,
    code_description STRING,
    code_type STRING NOT NULL,
    identifier STRING NOT NULL,
    reference_year INT NOT NULL,
    PRIMARY KEY (code_sk) NOT ENFORCED
) PARTITION BY RANGE_BUCKET(reference_year, GENERATE_ARRAY(2023, 2100, 1));

CREATE OR REPLACE TABLE `hca-takehome.reference.dim_date` (
        date_sk INT64 NOT NULL,
        full_date DATE NOT NULL,
        year INT64 NOT NULL,
        MONTH INT64 NOT NULL,
        YEAR_MONTH STRING NOT NULL,
        month_start_date DATE NOT NULL,
        PRIMARY KEY (date_sk) NOT ENFORCED
    );

--remove any existing data to prevent duplicates
TRUNCATE TABLE `hca-takehome.reference.dim_appendix_code`;

TRUNCATE TABLE `hca-takehome.reference.dim_date`;

INSERT INTO `hca-takehome.reference.dim_appendix_code` (
        code_sk,
        code,
        code_description,
        code_type,
        identifier,
        reference_year
    )
SELECT TO_HEX(
        SHA256(
            CONCAT(code, '|', identifier, '|', code_type, '|', 2025)
        )
    ) AS code_sk,
    code,
    code_description,
    code_type,
    identifier,
    2025 AS reference_year
FROM `hca-takehome.raw_ext.appendix_a_external`
UNION ALL
SELECT TO_HEX(
        SHA256(
            CONCAT(code, '|', identifier, '|', code_type, '|', 2025)
        )
    ) AS code_sk,
    code,
    code_description,
    code_type,
    identifier,
    2025 AS reference_year
FROM `hca-takehome.raw_ext.appendix_e_external`
UNION ALL
SELECT TO_HEX(
        SHA256(
            CONCAT(code, '|', identifier, '|', code_type, '|', 2025)
        )
    ) AS code_sk,
    code,
    code_description,
    code_type,
    identifier,
    2025 AS reference_year
FROM `hca-takehome.raw_ext.appendix_f_external`
UNION ALL
SELECT TO_HEX(
        SHA256(
            CONCAT(code, '|', identifier, '|', code_type, '|', 2025)
        )
    ) AS code_sk,
    code,
    code_description,
    code_type,
    identifier,
    2025 AS reference_year
FROM `hca-takehome.raw_ext.appendix_o_external`;

--CREATE row for NULL values for FK references
INSERT INTO `hca-takehome.reference.dim_appendix_code` (
        code_sk,
        code,
        code_description,
        code_type,
        identifier,
        reference_year
    )
SELECT TO_HEX(SHA256('UNKNOWN')) AS code_sk,
    'UNKNOWN' AS code,
    'UNKNOWN' AS code_description,
    code_type,
    'UNKNOWN' AS identifier,
    -1 AS reference_year
FROM UNNEST(
        [
  'DRG',
  'Diagnosis',
  'Procedure',
  'UNKNOWN'
]
    ) AS code_type
WHERE NOT EXISTS (
        SELECT 1
        FROM `hca-takehome.reference.dim_appendix_code` d
        WHERE d.code = 'UNKNOWN'
            AND d.code_type = code_type
    );

INSERT INTO `hca-takehome.reference.dim_date` (
        date_sk,
        full_date,
        year,
        MONTH,
        YEAR_MONTH,
        month_start_date
    ) WITH date_range AS (
        SELECT GENERATE_DATE_ARRAY(
                DATE '2020-01-01',
                DATE '2030-12-31',
                INTERVAL 1 DAY
            ) AS all_dates
    )
SELECT CAST(FORMAT_DATE('%Y%m%d', date) AS INT) AS date_sk,
    date AS full_date,
    EXTRACT(
        YEAR
        FROM date
    ) AS year,
    EXTRACT(
        MONTH
        FROM date
    ) AS MONTH,
    FORMAT_DATE('%Y-%m', date) AS YEAR_MONTH,
    DATE_TRUNC(date, MONTH) AS month_start_date
FROM date_range,
    UNNEST(all_dates) AS date;

--CREATE row for NULL values for FK references
INSERT INTO `hca-takehome.reference.dim_date` (
        date_sk,
        full_date,
        year,
        MONTH,
        YEAR_MONTH,
        month_start_date
    )
SELECT -1 AS date_sk,
    DATE '1900-01-01' AS full_date,
    -1 AS year,
    -1 AS MONTH,
    '1900-01' AS YEAR_MONTH,
    DATE '1900-01-01' AS month_start_date
FROM UNNEST([1]) AS dummy
WHERE NOT EXISTS (
        SELECT 1
        FROM `hca-takehome.reference.dim_date`
        WHERE date_sk = -1
    );