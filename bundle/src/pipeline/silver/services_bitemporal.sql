-- ------------------------------------------------------------------------------
-- Author:       Thomaz Antonio Rossito Neto
--               Principal Data Architect & AI Strategy
--               Databricks Solutions Architect Champion
-- Linkedin:     in/thomaz-antonio-rossito-neto/
-- Created:      2026-11-22
--
-- -----------------------------------------------------------------------------
-- Phase 3 · silver: the service catalog on two timelines (AUTO CDC bitemporal).
-- Business time (__START_AT, __END_AT) comes from deployed_at: when a version went live. System time
-- (__SYSTEM_START_AT, __SYSTEM_END_AT) comes from the commit in Postgres: since when the catalog said so.
-- The late record of 14:10 says 2.4.1 went live at 14:02: the table closes what it believed until 14:10 and keeps it,
-- so "what was live at 14:05" (2.4.1) and "what the catalog said at 14:05" (2.4.0) are both answerable.
-- The two sequence columns stay out of the table: the four period columns already carry them.

CREATE OR REFRESH STREAMING TABLE services_bitemporal
COMMENT 'Which version of each service was live (business time, __START_AT and __END_AT, from deployed_at) and since when the catalog said so (system time, __SYSTEM_START_AT and __SYSTEM_END_AT, from the Postgres commit). AUTO CDC bitemporal.';

CREATE FLOW services_bitemporal_cdc AS AUTO CDC INTO services_bitemporal
FROM STREAM(services_changes)
KEYS (service)
SEQUENCE BY deployed_at            -- business time: when the version went live
SYSTEM SEQUENCE BY committed_at    -- system time: when the catalog recorded it
COLUMNS * EXCEPT (op, lsn, deployed_at, committed_at, debezium_at, processed_at)
STORED AS BITEMPORAL;
