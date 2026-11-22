-- ------------------------------------------------------------------------------
-- Author:       Thomaz Antonio Rossito Neto
--               Principal Data Architect & AI Strategy
--               Databricks Solutions Architect Champion
-- Linkedin:     in/thomaz-antonio-rossito-neto/
-- Created:      2026-11-22
--
-- -----------------------------------------------------------------------------
-- Phase 3 · bitemporal in action, for the SQL editor: the two postmortem answers from the same table, at the same
-- moment, with the four period columns on the scenario clock (14:00 to 14:20) instead of UTC.
-- The business-time filter is the same in both halves; only the system-time filter changes.
-- Expected: 2.4.1 for "was live at 14:05" (live 14:02 to 14:14) and 2.4.0 for "catalog said at 14:05" (believed until
-- 14:10). "before the run" is earlier than the run of the sim; "open" is a period with no end yet.
-- zerobus_nrt_oncall is the default UC_CATALOG of .env.example.

USE CATALOG zerobus_nrt_oncall;

-- a time of the run on the scenario clock: the incident opened at 14:05 (second 300) and the sim ran at speed 12
CREATE OR REPLACE TEMPORARY FUNCTION scenario_clock(ts TIMESTAMP, opened TIMESTAMP)
RETURNS STRING
RETURN CASE
  WHEN ts IS NULL THEN 'open'
  WHEN ts < opened - INTERVAL 25 SECONDS THEN 'before the run'
  ELSE format_string('14:%02d', CAST(floor(round(300 + (unix_micros(ts) - unix_micros(opened)) * 12 / 1000000) / 60) AS INT))
END;

WITH incident AS (                -- the moment of the question: when the incident opened, 14:05 in the scenario
  SELECT max(opened_at) AS t
  FROM silver.incidents_current
  WHERE title = 'Checkout error rate above 5%'
),
answers AS (
  SELECT 'was live at 14:05' AS question, b.*
  FROM silver.services_bitemporal AS b, incident
  WHERE b.service = 'checkout'
    AND b.__START_AT <= t AND (b.__END_AT > t OR b.__END_AT IS NULL)   -- business time: at 14:05
    AND b.__SYSTEM_END_AT IS NULL                                        -- system time: what the catalog says today
  UNION ALL
  SELECT 'catalog said at 14:05', b.*
  FROM silver.services_bitemporal AS b, incident
  WHERE b.service = 'checkout'
    AND b.__START_AT <= t AND (b.__END_AT > t OR b.__END_AT IS NULL)   -- business time: at 14:05
    AND b.__SYSTEM_START_AT <= t                                         -- system time: what the catalog said at 14:05
    AND (b.__SYSTEM_END_AT > t OR b.__SYSTEM_END_AT IS NULL)
)
SELECT a.question, a.version,
       scenario_clock(a.__START_AT, i.t)        AS live_from,
       scenario_clock(a.__END_AT, i.t)          AS live_until,
       scenario_clock(a.__SYSTEM_START_AT, i.t) AS known_from,
       scenario_clock(a.__SYSTEM_END_AT, i.t)   AS known_until
FROM answers AS a, incident AS i;
