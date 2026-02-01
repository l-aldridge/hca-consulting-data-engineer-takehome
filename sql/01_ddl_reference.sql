-- Run order: 02 of 03
CREATE TABLE IF NOT EXISTS `hca-takehome.reference.dim_appendix_code` (
    code STRING NOT NULL,
    code_description STRING,
    code_type STRING NOT NULL,
    identifier STRING NOT NULL,
    PRIMARY KEY (code, identifier) NOT ENFORCED
);
INSERT INTO `hca-takehome`.`reference`.`dim_appendix_code` (code, code_description, code_type, identifier)
SELECT code,
    code_description,
    code_type,
    identifier
FROM `hca-takehome`.`raw_ext`.`appendix_a_external`
UNION ALL
SELECT code,
    code_description,
    code_type,
    identifier
FROM `hca-takehome`.`raw_ext`.`appendix_e_external`
UNION ALL
SELECT code,
    code_description,
    code_type,
    identifier
FROM `hca-takehome`.`raw_ext`.`appendix_f_external`
UNION ALL
SELECT code,
    code_description,
    code_type,
    identifier
FROM `hca-takehome`.`raw_ext`.`appendix_o_external`;