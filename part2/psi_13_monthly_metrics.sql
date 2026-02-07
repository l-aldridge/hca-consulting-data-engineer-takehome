-- list of sepsis codes from psi13.pdf. In production, pull these from a versioned reference table
DECLARE sepsis_codes ARRAY < STRING > DEFAULT [
  'A021','A4151','A227','A4152','A267','A4153','A327','A4154','A400','A4159',
  'A401','A4181','A403','A4189','A408','A419','A409','A427','A4101','A5486',
  'A4102','B377','A411','R6520','A412','R6521','A413','T8112XA','A414','T8144XA','A4150'
];

CREATE OR REPLACE TABLE `hca-takehome.analytics.psi13_monthly_metrics` AS WITH denominator_inclusions AS (
    SELECT DISTINCT d.patient_id,
      rd.year_month
    FROM `hca-takehome.core.fct_discharge` d
      JOIN `hca-takehome.reference.dim_date` rd ON rd.date_sk = d.discharge_date_sk -- elective adult surgical discharges with OR procedure, in 2025
    WHERE d.is_elective = 1
      AND d.is_adult = 1
      AND rd.year = 2025
      AND d.drg IS NOT NULL
      AND d.drg != '999' -- DRG must be tagged SURGI2R (via bridge)
      AND EXISTS (
        SELECT 1
        FROM `hca-takehome.reference.bridge_code_identifier` b
        WHERE b.code_sk = d.drg_code_sk
          AND b.code_type = 'DRG'
          AND b.reference_year = 2025
          AND b.identifier = 'SURGI2R'
      ) -- must have at least one procedure tagged ORPROC (via bridge)
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
            AND b.identifier IN ('SEPTI2D', 'INFECID', 'MDC14PRINDX', 'MDC15PRINDX')
        )
      )
      OR -- Exclude if secondary dx POA=Y and tagged SEPTI2D or INFECID
      (
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
      dd.diag_code IN UNNEST(sepsis_codes)
      AND dd.is_principal_diag = 0,
      di.patient_id,
      NULL
    )
  ) AS numerator,
  COUNT(DISTINCT di.patient_id) AS denominator,
  ROUND(
    SAFE_DIVIDE(
      COUNT(
        DISTINCT IF(
          dd.diag_code IN UNNEST(sepsis_codes)
          AND dd.is_principal_diag = 0,
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