CREATE SCHEMA IF NOT EXISTS `hca-takehome.raw_ext` OPTIONS(
    description = "This schema contains raw external data ingested from CSV files for the HCA Consulting takehome exercise.",
    location = "US"
);

CREATE SCHEMA IF NOT EXISTS `hca-takehome.core` OPTIONS(
    description = "This schema contains modeled relational tables from the core dataset.",
    location = "US"
);

CREATE SCHEMA IF NOT EXISTS `hca-takehome.reference` OPTIONS(
    description = "This schema contains modeled relational tables from the reference dataset.",
    location = "US"
);

CREATE SCHEMA IF NOT EXISTS `hca-takehome.analytics` OPTIONS(
    description = "This schema contains transformed and aggregated tables for analytics purposes.",
    location = "US"
);

CREATE OR REPLACE EXTERNAL TABLE `hca-takehome.raw_ext.patient_external` (
        patient_id STRING,
        drg STRING,
        birth_date STRING,
        admission_date STRING,
        discharge_date STRING,
        admission_type STRING
    ) OPTIONS (
        format = 'CSV',
        uris = ['gs://hca-analytics-raw-dev/core/patient.csv'],
        skip_leading_rows = 1,
        field_delimiter = ',',
        quote = '"',
        allow_quoted_newlines = TRUE
    );

CREATE OR REPLACE EXTERNAL TABLE `hca-takehome.raw_ext.patient_procedure_external` (
        patient_id STRING,
        procedure_code STRING
    ) OPTIONS (
        format = 'CSV',
        uris = ['gs://hca-analytics-raw-dev/core/patient_procedure.csv'],
        skip_leading_rows = 1,
        field_delimiter = ',',
        quote = '"',
        allow_quoted_newlines = TRUE
    );

CREATE OR REPLACE EXTERNAL TABLE `hca-takehome.raw_ext.patient_diagnosis_external` (
        patient_id STRING,
        diag_code STRING,
        diag_rank_num INT,
        present_on_admission_ind STRING
    ) OPTIONS (
        format = 'CSV',
        uris = ['gs://hca-analytics-raw-dev/core/patient_diagnosis.csv'],
        skip_leading_rows = 1,
        field_delimiter = ',',
        quote = '"',
        allow_quoted_newlines = TRUE
    );

CREATE OR REPLACE EXTERNAL TABLE `hca-takehome.raw_ext.appendix_a_external` (
        code STRING,
        code_description STRING,
        code_type STRING,
        identifier STRING
    ) OPTIONS (
        format = 'CSV',
        uris = ['gs://hca-analytics-raw-dev/reference/appendix_a.csv'],
        skip_leading_rows = 1,
        field_delimiter = ',',
        quote = '"',
        allow_quoted_newlines = TRUE
    );

CREATE OR REPLACE EXTERNAL TABLE `hca-takehome.raw_ext.appendix_e_external` (
        code STRING,
        code_description STRING,
        code_type STRING,
        identifier STRING
    ) OPTIONS (
        format = 'CSV',
        uris = ['gs://hca-analytics-raw-dev/reference/appendix_e.csv'],
        skip_leading_rows = 1,
        field_delimiter = ',',
        quote = '"',
        allow_quoted_newlines = TRUE
    );

CREATE OR REPLACE EXTERNAL TABLE `hca-takehome.raw_ext.appendix_f_external` (
        code STRING,
        code_description STRING,
        code_type STRING,
        identifier STRING
    ) OPTIONS (
        format = 'CSV',
        uris = ['gs://hca-analytics-raw-dev/reference/appendix_f.csv'],
        skip_leading_rows = 1,
        field_delimiter = ',',
        quote = '"',
        allow_quoted_newlines = TRUE
    );

CREATE OR REPLACE EXTERNAL TABLE `hca-takehome.raw_ext.appendix_o_external` (
        code STRING,
        code_description STRING,
        code_type STRING,
        identifier STRING
    ) OPTIONS (
        format = 'CSV',
        uris = ['gs://hca-analytics-raw-dev/reference/appendix_o.csv'],
        skip_leading_rows = 1,
        field_delimiter = ',',
        quote = '"',
        allow_quoted_newlines = TRUE
    );

CREATE OR REPLACE EXTERNAL TABLE `hca-takehome.raw_ext.sepsis_codes` (
        code STRING,
        code_description STRING,
        code_type STRING,
        identifier STRING
    ) OPTIONS (
        format = 'CSV',
        uris = ['gs://hca-analytics-raw-dev/reference/sepsis_codes.csv'],
        skip_leading_rows = 1,
        field_delimiter = ',',
        quote = '"',
        allow_quoted_newlines = TRUE
    );