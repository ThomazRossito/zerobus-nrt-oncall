-- ------------------------------------------------------------------------------
-- Author:       Thomaz Antonio Rossito Neto
--               Principal Data Architect & AI Strategy
--               Databricks Solutions Architect Champion
-- Linkedin:     in/thomaz-antonio-rossito-neto/
-- Created:      2026-11-20
--
-- -----------------------------------------------------------------------------
-- Phase 2 · bronze of the CDC side: one row per change, written by Debezium Server through Zerobus (typed mode).
-- Each table has the Postgres columns, then what the ExtractNewRecordState SMT adds: __op, __ts_ms, __source_ts_ms,
-- __lsn and __deleted. timestamptz arrives as an ISO-8601 string (io.debezium.time.ZonedTimestamp) and Zerobus takes
-- TIMESTAMP only as epoch microseconds, so bronze keeps those columns as STRING and silver converts them.
-- Run by "python setup/02_cdc.py create", with the project catalog as the default catalog.

CREATE TABLE IF NOT EXISTS bronze.services (
  service         STRING,
  version         STRING,
  deployed_at     STRING  COMMENT 'business time: when the version went live (timestamptz as ISO-8601)',
  owner           STRING,
  __op            STRING  COMMENT 'r snapshot read, c insert, u update, d delete',
  __ts_ms         BIGINT  COMMENT 'when Debezium processed the change (epoch ms)',
  __source_ts_ms  BIGINT  COMMENT 'commit time in Postgres (epoch ms): the system time of the catalog',
  __lsn           BIGINT  COMMENT 'position in the Postgres WAL',
  __deleted       STRING  COMMENT 'true on deletes'
) COMMENT 'Phase 2: changes of cmdb.services, one row per change';

CREATE TABLE IF NOT EXISTS bronze.incidents (
  incident_id        BIGINT,
  service            STRING,
  severity           STRING,
  status             STRING,
  title              STRING,
  suspected_version  STRING,
  opened_at          STRING  COMMENT 'timestamptz as ISO-8601',
  updated_at         STRING  COMMENT 'timestamptz as ISO-8601',
  __op               STRING  COMMENT 'r snapshot read, c insert, u update, d delete',
  __ts_ms            BIGINT  COMMENT 'when Debezium processed the change (epoch ms)',
  __source_ts_ms     BIGINT  COMMENT 'commit time in Postgres (epoch ms)',
  __lsn              BIGINT  COMMENT 'position in the Postgres WAL',
  __deleted          STRING  COMMENT 'true on deletes'
) COMMENT 'Phase 2: changes of cmdb.incidents, one row per change';

CREATE TABLE IF NOT EXISTS bronze.alerts_history (
  alert_id        BIGINT,
  service         STRING,
  rule            STRING,
  severity        STRING,
  fired_at        STRING  COMMENT 'timestamptz as ISO-8601',
  value           DOUBLE,
  __op            STRING  COMMENT 'r snapshot read, c insert, u update, d delete',
  __ts_ms         BIGINT  COMMENT 'when Debezium processed the change (epoch ms)',
  __source_ts_ms  BIGINT  COMMENT 'commit time in Postgres (epoch ms)',
  __lsn           BIGINT  COMMENT 'position in the Postgres WAL',
  __deleted       STRING  COMMENT 'true on deletes'
) COMMENT 'Phase 2: initial load of cmdb.alerts_history for the debezium/dbz#3026 test, it does not go to silver';
