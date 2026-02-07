-- WARNING:
-- This script DROPS all objects created by the take-home exercise.
-- Drop analytics tables
DROP TABLE IF EXISTS `hca-takehome.analytics.discharge_summary`;

DROP TABLE IF EXISTS `hca-takehome.analytics.fct_procedure`;

DROP TABLE IF EXISTS `hca-takehome.analytics.fct_diagnosis`;

DROP TABLE IF EXISTS `hca-takehome.analytics.fct_discharge`;

-- Drop core tables
DROP TABLE IF EXISTS `hca-takehome.core.patient_diagnosis`;

DROP TABLE IF EXISTS `hca-takehome.core.patient_procedure`;

DROP TABLE IF EXISTS `hca-takehome.core.patient`;

-- Drop reference tables
DROP TABLE IF EXISTS `hca-takehome.reference.ref_appendix_code_identifier_map`;

DROP TABLE IF EXISTS `hca-takehome.reference.dim_code`;

DROP TABLE IF EXISTS `hca-takehome.reference.bridge_code_identifier`;

DROP TABLE IF EXISTS `hca-takehome.reference.dim_date`;

--Drop external tables
DROP TABLE IF EXISTS `hca-takehome.raw_ext.patient_external`;

DROP TABLE IF EXISTS `hca-takehome.raw_ext.patient_procedure_external`;

DROP TABLE IF EXISTS `hca-takehome.raw_ext.patient_diagnosis_external`;

DROP TABLE IF EXISTS `hca-takehome.raw_ext.appendix_a_external`;

DROP TABLE IF EXISTS `hca-takehome.raw_ext.appendix_e_external`;

DROP TABLE IF EXISTS `hca-takehome.raw_ext.appendix_f_external`;

DROP TABLE IF EXISTS `hca-takehome.raw_ext.appendix_o_external`;

-- Drop schemas
DROP SCHEMA IF EXISTS `hca-takehome.analytics`;

DROP SCHEMA IF EXISTS `hca-takehome.core`;

DROP SCHEMA IF EXISTS `hca-takehome.reference`;

DROP SCHEMA IF EXISTS `hca-takehome.raw_ext`;