-- Run order: 02 of 03
CREATE TABLE IF NOT EXISTS `hca-takehome.reference.ref_appendix_code_identifier_map` (
    code_identifier_sk STRING NOT NULL,
    code STRING NOT NULL,
    code_description STRING,
    code_type STRING NOT NULL,
    identifier STRING NOT NULL,
    reference_year INT NOT NULL,
    PRIMARY KEY (code_identifier_sk) NOT ENFORCED
) PARTITION BY RANGE_BUCKET(reference_year, GENERATE_ARRAY(2023, 2100, 1));

CREATE TABLE IF NOT EXISTS `hca-takehome.reference.dim_code` (
    code_sk STRING NOT NULL,
    code STRING NOT NULL,
    code_type STRING NOT NULL,
    code_description STRING,
    reference_year INT NOT NULL,
    PRIMARY KEY (code_sk) NOT ENFORCED
) PARTITION BY RANGE_BUCKET(reference_year, GENERATE_ARRAY(2023, 2100, 1)) CLUSTER BY code_type,
code;

CREATE TABLE IF NOT EXISTS `hca-takehome.reference.bridge_code_identifier` (
    code_sk STRING NOT NULL,
    identifier_sk STRING NOT NULL,
    identifier STRING NOT NULL,
    code_type STRING NOT NULL,
    reference_year INT NOT NULL,
    PRIMARY KEY (code_sk, identifier_sk, reference_year) NOT ENFORCED
) PARTITION BY RANGE_BUCKET(reference_year, GENERATE_ARRAY(2023, 2100, 1)) CLUSTER BY identifier,
code_type;

CREATE OR REPLACE TABLE `hca-takehome.reference.dim_date` (
        date_sk INT64 NOT NULL,
        full_date DATE NOT NULL,
        year INT64 NOT NULL,
        MONTH INT64 NOT NULL,
        YEAR_MONTH STRING NOT NULL,
        month_start_date DATE NOT NULL,
        PRIMARY KEY (date_sk) NOT ENFORCED
    );

-- remove any existing data to prevent duplicates
TRUNCATE TABLE `hca-takehome.reference.ref_appendix_code_identifier_map`;

TRUNCATE TABLE `hca-takehome.reference.dim_code`;

TRUNCATE TABLE `hca-takehome.reference.bridge_code_identifier`;

TRUNCATE TABLE `hca-takehome.reference.dim_date`;

INSERT INTO `hca-takehome.reference.ref_appendix_code_identifier_map` (
        code_identifier_sk,
        code,
        code_description,
        code_type,
        identifier,
        reference_year
    )
SELECT TO_HEX(
        SHA256(
            CONCAT(
                code,
                '|',
                identifier,
                '|',
                code_type,
                '|',
                '2025'
            )
        )
    ) AS code_identifier_sk,
    code,
    code_description,
    code_type,
    identifier,
    2025 AS reference_year
FROM `hca-takehome.raw_ext.appendix_a_external`
UNION ALL
SELECT TO_HEX(
        SHA256(
            CONCAT(
                code,
                '|',
                identifier,
                '|',
                code_type,
                '|',
                '2025'
            )
        )
    ) AS code_identifier_sk,
    code,
    code_description,
    code_type,
    identifier,
    2025 AS reference_year
FROM `hca-takehome.raw_ext.appendix_e_external`
UNION ALL
SELECT TO_HEX(
        SHA256(
            CONCAT(
                code,
                '|',
                identifier,
                '|',
                code_type,
                '|',
                '2025'
            )
        )
    ) AS code_identifier_sk,
    code,
    code_description,
    code_type,
    identifier,
    2025 AS reference_year
FROM `hca-takehome.raw_ext.appendix_f_external`
UNION ALL
SELECT TO_HEX(
        SHA256(
            CONCAT(
                code,
                '|',
                identifier,
                '|',
                code_type,
                '|',
                '2025'
            )
        )
    ) AS code_identifier_sk,
    code,
    code_description,
    code_type,
    identifier,
    2025 AS reference_year
FROM `hca-takehome.raw_ext.appendix_o_external`;

-- CREATE row for NULL values for FK references
INSERT INTO `hca-takehome.reference.ref_appendix_code_identifier_map` (
        code_identifier_sk,
        code,
        code_description,
        code_type,
        identifier,
        reference_year
    ) WITH years AS (
        SELECT DISTINCT reference_year
        FROM `hca-takehome.reference.ref_appendix_code_identifier_map`
        WHERE reference_year IS NOT NULL
        UNION ALL
        SELECT -1
    ),
    TYPES AS (
        SELECT code_type
        FROM UNNEST(['DRG', 'Diagnosis', 'Procedure', 'UNKNOWN']) AS code_type
    )
SELECT TO_HEX(
        SHA256(
            CONCAT(
                t.code_type,
                '|',
                'UNKNOWN',
                '|',
                'UNKNOWN',
                '|',
                CAST(y.reference_year AS STRING)
            )
        )
    ) AS code_identifier_sk,
    'UNKNOWN' AS code,
    'UNKNOWN' AS code_description,
    t.code_type,
    'UNKNOWN' AS identifier,
    y.reference_year
FROM TYPES t
    CROSS JOIN years y
WHERE NOT EXISTS (
        SELECT 1
        FROM `hca-takehome.reference.ref_appendix_code_identifier_map` d
        WHERE d.code = 'UNKNOWN'
            AND d.code_type = t.code_type
            AND d.reference_year = y.reference_year
    );

INSERT INTO `hca-takehome.reference.dim_code` (
        code_sk,
        code,
        code_type,
        code_description,
        reference_year
    )
SELECT TO_HEX(
        SHA256(
            CONCAT(
                code,
                '|',
                code_type,
                '|',
                CAST(reference_year AS STRING)
            )
        )
    ) AS code_sk,
    code,
    code_type,
    ANY_VALUE(code_description) AS code_description,
    reference_year
FROM `hca-takehome.reference.ref_appendix_code_identifier_map`
GROUP BY code,
    code_type,
    reference_year;

INSERT INTO `hca-takehome.reference.bridge_code_identifier` (
        code_sk,
        identifier_sk,
        identifier,
        code_type,
        reference_year
    )
SELECT DISTINCT dc.code_sk,
    TO_HEX(
        SHA256(
            CONCAT(
                a.identifier,
                '|',
                a.code_type,
                '|',
                CAST(a.reference_year AS STRING)
            )
        )
    ) AS identifier_sk,
    a.identifier,
    a.code_type,
    a.reference_year
FROM `hca-takehome.reference.ref_appendix_code_identifier_map` a
    JOIN `hca-takehome.reference.dim_code` dc ON dc.code = a.code
    AND dc.code_type = a.code_type
    AND dc.reference_year = a.reference_year;

INSERT INTO `hca-takehome.reference.dim_date` (
        date_sk,
        full_date,
        year,
        MONTH,
        YEAR_MONTH,
        month_start_date
    ) WITH date_range AS (
        SELECT GENERATE_DATE_ARRAY(
                DATE '1850-01-01',
                DATE '2030-12-31',
                INTERVAL 1 DAY
            ) AS all_dates
    )
SELECT CAST(FORMAT_DATE('%Y%m%d', d) AS INT64) AS date_sk,
    d AS full_date,
    EXTRACT(
        YEAR
        FROM d
    ) AS year,
    EXTRACT(
        MONTH
        FROM d
    ) AS MONTH,
    FORMAT_DATE('%Y-%m', d) AS YEAR_MONTH,
    DATE_TRUNC(d, MONTH) AS month_start_date
FROM date_range,
    UNNEST(all_dates) AS d;

-- CREATE row for NULL values for FK references
INSERT INTO `hca-takehome.reference.dim_date` (
        date_sk,
        full_date,
        year,
        MONTH,
        YEAR_MONTH,
        month_start_date
    )
SELECT -1 AS date_sk,
    DATE '1800-01-01' AS full_date,
    -1 AS year,
    -1 AS MONTH,
    '1800-01' AS YEAR_MONTH,
    DATE '1800-01-01' AS month_start_date
FROM UNNEST([1]) AS dummy
WHERE NOT EXISTS (
        SELECT 1
        FROM `hca-takehome.reference.dim_date`
        WHERE date_sk = -1
    );