-- ------------------------------------------------------------------------------
-- Author:       Thomaz Antonio Rossito Neto
--               Principal Data Architect & AI Strategy
--               Databricks Solutions Architect Champion
-- Linkedin:     in/thomaz-antonio-rossito-neto/
-- Created:      2026-11-22
--
-- -----------------------------------------------------------------------------
-- Phase 3 · silver: the incidents as they are now (AUTO CDC SCD type 1), deletes applied.
-- Sequenced by the WAL position (lsn), the exact order in which Postgres changed each row. A delete arrives with the
-- key only and removes the row: the duplicate incident of the scenario is created at 14:06 and deleted at 14:07.
-- committed_at stays in the table as the commit of the incident's last change.

CREATE OR REFRESH STREAMING TABLE incidents_current
COMMENT 'Current state of each incident (AUTO CDC SCD type 1): deleted incidents are gone, and committed_at is the commit of the last change.';

CREATE FLOW incidents_current_cdc AS AUTO CDC INTO incidents_current
FROM STREAM(incidents_changes)
KEYS (incident_id)
APPLY AS DELETE WHEN op = 'd'
SEQUENCE BY lsn
COLUMNS * EXCEPT (op, lsn, debezium_at, processed_at)
STORED AS SCD TYPE 1;
