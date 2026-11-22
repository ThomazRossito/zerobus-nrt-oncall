-- ------------------------------------------------------------------------------
-- Author:       Thomaz Antonio Rossito Neto
--               Principal Data Architect & AI Strategy
--               Databricks Solutions Architect Champion
-- Linkedin:     in/thomaz-antonio-rossito-neto/
-- Created:      2026-11-22
--
-- -----------------------------------------------------------------------------
-- Phase 3 · bitemporal in action, for the SQL editor: the two postmortem answers from the same table, at the same
-- moment. The business-time filter is the same in both halves; only the system-time filter changes.
-- Expected: 2.4.1 for "was live at 14:05" and 2.4.0 for "catalog said at 14:05".
-- zerobus_nrt_oncall is the default UC_CATALOG of .env.example.

USE CATALOG zerobus_nrt_oncall;

WITH incident AS (                -- the moment of the question: when the incident opened, 14:05 in the scenario
  SELECT max(opened_at) AS t
  FROM silver.incidents_current
  WHERE title = 'Checkout error rate above 5%'
)
SELECT 'was live at 14:05' AS question, version
FROM silver.services_bitemporal, incident
WHERE service = 'checkout'
  AND __START_AT <= t AND (__END_AT > t OR __END_AT IS NULL)   -- business time: at 14:05
  AND __SYSTEM_END_AT IS NULL                                    -- system time: what the catalog says today
UNION ALL
SELECT 'catalog said at 14:05', version
FROM silver.services_bitemporal, incident
WHERE service = 'checkout'
  AND __START_AT <= t AND (__END_AT > t OR __END_AT IS NULL)   -- business time: at 14:05
  AND __SYSTEM_START_AT <= t                                     -- system time: what the catalog said at 14:05
  AND (__SYSTEM_END_AT > t OR __SYSTEM_END_AT IS NULL);
