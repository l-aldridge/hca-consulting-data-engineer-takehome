-- Sepsis diagnosis codes sourced from reference tables (identifier = 'SEPTI2D')
sepsis_diag_codes AS (
  CREATE OR REPLACE TABLE `hca-takehome.analytics.psi13_monthly_metrics` AS WITH
  SELECT DISTINCT b.code_sk
  FROM `hca-takehome.reference.bridge_code_identifier` b
  WHERE b.code_type = 'Diagnosis'
    AND b.reference_year = 2025
    AND b.identifier = 'SEPTI2D'
),
denominator_inclusions AS (
  SELECT DISTINCT d.patient_id,
    rd.year_month
  FROM `hca-takehome.core.fct_discharge` d
    JOIN `hca-takehome.reference.dim_date` rd ON rd.date_sk = d.discharge_date_sk
  WHERE d.is_elective = 1
    AND d.is_adult = 1
    AND rd.year = 2025
    AND d.drg IS NOT NULL
    AND d.drg != '999'
    AND EXISTS (
      SELECT 1
      FROM `hca-takehome.reference.bridge_code_identifier` b
      WHERE b.code_sk = d.drg_code_sk
        AND b.code_type = 'DRG'
        AND b.reference_year = 2025
        AND b.identifier = 'SURGI2R'
    )
    AND EXISTS (
      SELECT 1
      FROM `hca-takehome.core.fct_procedure` p
      WHERE p.patient_id = d.patient_id
        AND EXISTS (
          SELECT 1
          FROM `hca-takehome.reference.bridge_code_identifier` b
          WHERE b.code_sk = p.procedure_code_sk
            AND b.code_type = 'Procedure'
            AND b.reference_year = 2025
            AND b.identifier = 'ORPROC'
        )
    )
),
denominator_exclusions AS (
  SELECT DISTINCT d.patient_id
  FROM `hca-takehome.core.fct_discharge` d
    LEFT JOIN `hca-takehome.core.fct_diagnosis` di ON di.patient_id = d.patient_id
  WHERE -- Exclude if principal diagnosis has any of these identifiers
    (
      di.is_principal_diag = 1
      AND EXISTS (
        SELECT 1
        FROM `hca-takehome.reference.bridge_code_identifier` b
        WHERE b.code_sk = di.diag_code_sk
          AND b.code_type = 'Diagnosis'
          AND b.reference_year = 2025
          AND b.identifier IN (
            'SEPTI2D',
            'INFECID',
            'MDC14PRINDX',
            'MDC15PRINDX'
          )
      )
    ) (
      di.is_principal_diag = 0
      AND di.is_present_on_admission = 1
      AND EXISTS (
        SELECT 1
        FROM `hca-takehome.reference.bridge_code_identifier` b
        WHERE b.code_sk = di.diag_code_sk
          AND b.code_type = 'Diagnosis'
          AND b.reference_year = 2025
          AND b.identifier IN ('SEPTI2D', 'INFECID')
      )
    )
    OR d.drg IS NULL
    OR d.drg = '999'
)
SELECT di.year_month AS MONTH,
  COUNT(
    DISTINCT IF(
      dd.is_principal_diag = 0
      AND EXISTS (
        SELECT 1
        FROM sepsis_diag_codes s
        WHERE s.code_sk = dd.diag_code_sk
      ),
      di.patient_id,
      NULL
    )
  ) AS numerator,
  COUNT(DISTINCT di.patient_id) AS denominator,
  ROUND(
    SAFE_DIVIDE(
      COUNT(
        DISTINCT IF(
          dd.is_principal_diag = 0
          AND EXISTS (
            SELECT 1
            FROM sepsis_diag_codes s
            WHERE s.code_sk = dd.diag_code_sk
          ),
          di.patient_id,
          NULL
        )
      ),
      COUNT(DISTINCT di.patient_id)
    ),
    3
  ) AS rate
FROM denominator_inclusions di
  LEFT JOIN denominator_exclusions de ON de.patient_id = di.patient_id
  LEFT JOIN `hca-takehome.core.fct_diagnosis` dd ON dd.patient_id = di.patient_id
WHERE de.patient_id IS NULL
GROUP BY di.year_month
ORDER BY di.year_month ASC;