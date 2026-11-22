-- ------------------------------------------------------------------------------
-- Author:       Thomaz Antonio Rossito Neto
--               Principal Data Architect & AI Strategy
--               Databricks Solutions Architect Champion
-- Linkedin:     in/thomaz-antonio-rossito-neto/
-- Created:      2026-11-22
--
-- -----------------------------------------------------------------------------
-- Phase 3 · gold: the latency of each change on the CDC path, one row per insert, update or delete. The initial
-- snapshot rows stay out: their commit time is the time of the snapshot, not of a change.
-- capture_ms: commit in Postgres -> Debezium processed it. to_silver_ms: commit -> the silver micro-batch, which
-- includes Debezium, Zerobus, the ~5 s until bronze is queryable and the wait for the pipeline. The pipeline runs
-- triggered in Phase 3, so to_silver_ms also counts the time until an update started; it measures latency only with
-- the pipeline continuous (Phase 6). The commit -> bronze hop alone is measured by measure/latency.py.

CREATE OR REFRESH STREAMING TABLE gold.hop_latency
COMMENT 'Latency of each change on the CDC path, per insert, update or delete: capture_ms (commit to Debezium) and to_silver_ms (commit to silver). to_silver_ms measures the pipeline only when it runs continuous.';

CREATE FLOW hop_latency_services AS INSERT INTO gold.hop_latency BY NAME
SELECT
  'services'                                            AS source_table,
  service                                               AS record_key,
  op,
  lsn,
  committed_at,
  debezium_at,
  processed_at,
  unix_millis(debezium_at) - unix_millis(committed_at)  AS capture_ms,
  unix_millis(processed_at) - unix_millis(committed_at) AS to_silver_ms
FROM STREAM(silver.services_changes)
WHERE op <> 'r';

CREATE FLOW hop_latency_incidents AS INSERT INTO gold.hop_latency BY NAME
SELECT
  'incidents'                                           AS source_table,
  CAST(incident_id AS STRING)                           AS record_key,
  op,
  lsn,
  committed_at,
  debezium_at,
  processed_at,
  unix_millis(debezium_at) - unix_millis(committed_at)  AS capture_ms,
  unix_millis(processed_at) - unix_millis(committed_at) AS to_silver_ms
FROM STREAM(silver.incidents_changes)
WHERE op <> 'r';
