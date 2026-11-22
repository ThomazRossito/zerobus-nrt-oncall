-- ------------------------------------------------------------------------------
-- Author:       Thomaz Antonio Rossito Neto
--               Principal Data Architect & AI Strategy
--               Databricks Solutions Architect Champion
-- Linkedin:     in/thomaz-antonio-rossito-neto/
-- Created:      2026-11-22
--
-- -----------------------------------------------------------------------------
-- Phase 3 · silver: every change Debezium captured, one row per change, with the time of each hop.
-- Reads the bronze tables Zerobus writes (sql/phase2/bronze.sql): the Postgres row plus __op (r snapshot, c insert,
-- u update, d delete), __lsn (position in the WAL), __source_ts_ms (the commit in Postgres) and __ts_ms (when Debezium
-- processed the change). timestamptz columns arrive as ISO-8601 text and become TIMESTAMP here.
-- The hops: committed_at (commit in Postgres) -> debezium_at (Debezium processed it) -> processed_at (the micro-batch
-- of this table, a lower bound of when the row reached silver). The AUTO CDC flows read these two tables.

CREATE OR REFRESH STREAMING TABLE services_changes (
  CONSTRAINT known_op        EXPECT (op IN ('r', 'c', 'u', 'd')),
  CONSTRAINT has_lsn         EXPECT (lsn IS NOT NULL)          ON VIOLATION FAIL UPDATE,
  CONSTRAINT has_commit_time EXPECT (committed_at IS NOT NULL) ON VIOLATION FAIL UPDATE,
  -- the business time of the bitemporal table. The catalog never deletes a service in this experiment; a delete
  -- would arrive with the key only (REPLICA IDENTITY DEFAULT), so the update stops here instead of in AUTO CDC
  CONSTRAINT has_live_time   EXPECT (deployed_at IS NOT NULL)  ON VIOLATION FAIL UPDATE
)
COMMENT 'Every change to the service catalog (cmdb.services) as Debezium captured it, with the time of each hop.'
AS SELECT
  service,
  version,
  CAST(deployed_at AS TIMESTAMP)   AS deployed_at,
  owner,
  __op                             AS op,
  __lsn                            AS lsn,
  timestamp_millis(__source_ts_ms) AS committed_at,
  timestamp_millis(__ts_ms)        AS debezium_at,
  current_timestamp()              AS processed_at
FROM STREAM(bronze.services);

CREATE OR REFRESH STREAMING TABLE incidents_changes (
  CONSTRAINT known_op        EXPECT (op IN ('r', 'c', 'u', 'd')),
  CONSTRAINT has_lsn         EXPECT (lsn IS NOT NULL)          ON VIOLATION FAIL UPDATE,
  CONSTRAINT has_commit_time EXPECT (committed_at IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT 'Every change to the incidents (cmdb.incidents) as Debezium captured it, with the time of each hop. A delete carries the key only.'
AS SELECT
  incident_id,
  service,
  severity,
  status,
  title,
  suspected_version,
  CAST(opened_at AS TIMESTAMP)     AS opened_at,
  CAST(updated_at AS TIMESTAMP)    AS updated_at,
  __op                             AS op,
  __lsn                            AS lsn,
  timestamp_millis(__source_ts_ms) AS committed_at,
  timestamp_millis(__ts_ms)        AS debezium_at,
  current_timestamp()              AS processed_at
FROM STREAM(bronze.incidents);
