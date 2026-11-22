-- ------------------------------------------------------------------------------
-- Author:       Thomaz Antonio Rossito Neto
--               Principal Data Architect & AI Strategy
--               Databricks Solutions Architect Champion
-- Linkedin:     in/thomaz-antonio-rossito-neto/
-- Created:      2026-11-22
--
-- -----------------------------------------------------------------------------
-- Phase 3 · gold: one row per incident in silver.incidents_current, with the two answers of the postmortem: the
-- version that was live when it opened (business time, today's knowledge) and the version the catalog named at that
-- moment (the bitemporal as-of), next to what on-call suspected at open, what the spans carried and when the status
-- changed. Incidents that only came in the initial snapshot have no insert or status change in the CDC, so those
-- columns are empty for them; durations are real seconds (at speed 12, one real second is 12 on the scenario clock).

CREATE OR REFRESH MATERIALIZED VIEW gold.incident_postmortem
COMMENT 'One row per incident with the two postmortem answers: the version that was live when it opened and the version the catalog named at that moment (bitemporal as-of), plus what was suspected, what the spans carried and when the status changed.'
AS
WITH opened AS (
  -- the suspected version written when the incident was created
  SELECT incident_id, min_by(suspected_version, lsn) AS suspected_at_open
  FROM silver.incidents_changes
  WHERE op = 'c'
  GROUP BY incident_id
),
statuses AS (
  -- the first commit of each status, from the inserts and updates
  SELECT
    incident_id,
    min(committed_at) FILTER (WHERE status = 'investigating') AS investigating_at,
    min(committed_at) FILTER (WHERE status = 'mitigated')     AS mitigated_at,
    min(committed_at) FILTER (WHERE status = 'resolved')      AS resolved_at
  FROM silver.incidents_changes
  WHERE op IN ('c', 'u')
  GROUP BY incident_id
),
telemetry AS (
  -- the version that served most requests of the incident's service in the bucket where it opened
  SELECT i.incident_id, max_by(h.version, h.requests) AS telemetry_version_at_open
  FROM silver.incidents_current AS i
  JOIN gold.service_health AS h
    ON h.service = i.service AND h.bucket_start <= i.opened_at AND i.opened_at < h.bucket_end
  GROUP BY i.incident_id
)
SELECT
  i.incident_id,
  i.service,
  i.severity,
  i.status,
  i.title,
  i.opened_at,
  o.suspected_at_open,
  known.version                                      AS catalog_known_at_open,
  live.version                                       AS catalog_live_at_open,
  t.telemetry_version_at_open,
  i.suspected_version                                AS suspected_now,
  s.investigating_at,
  s.mitigated_at,
  s.resolved_at,
  timestampdiff(SECOND, i.opened_at, s.mitigated_at) AS seconds_to_mitigate,
  timestampdiff(SECOND, i.opened_at, s.resolved_at)  AS seconds_to_resolve
FROM silver.incidents_current AS i
LEFT JOIN opened AS o ON o.incident_id = i.incident_id
LEFT JOIN statuses AS s ON s.incident_id = i.incident_id
LEFT JOIN telemetry AS t ON t.incident_id = i.incident_id
LEFT JOIN silver.services_bitemporal AS live
  ON  live.service = i.service
  AND live.__START_AT <= i.opened_at
  AND (live.__END_AT > i.opened_at OR live.__END_AT IS NULL)
  AND live.__SYSTEM_END_AT IS NULL
LEFT JOIN silver.services_bitemporal AS known
  ON  known.service = i.service
  AND known.__START_AT <= i.opened_at
  AND (known.__END_AT > i.opened_at OR known.__END_AT IS NULL)
  AND known.__SYSTEM_START_AT <= i.opened_at
  AND (known.__SYSTEM_END_AT > i.opened_at OR known.__SYSTEM_END_AT IS NULL);
