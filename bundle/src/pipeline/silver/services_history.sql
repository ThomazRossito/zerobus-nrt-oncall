-- ------------------------------------------------------------------------------
-- Author:       Thomaz Antonio Rossito Neto
--               Principal Data Architect & AI Strategy
--               Databricks Solutions Architect Champion
-- Linkedin:     in/thomaz-antonio-rossito-neto/
-- Created:      2026-11-22
--
-- -----------------------------------------------------------------------------
-- Phase 3 · silver: the history of the service catalog in the order it was recorded (AUTO CDC SCD type 2).
-- Sequenced by the commit in Postgres, so __START_AT and __END_AT are commit times: 2.4.1 starts at 14:10, when the
-- catalog learned about it, not at 14:02, when it went live. That one timeline is what bitemporal adds to.

CREATE OR REFRESH STREAMING TABLE services_history
COMMENT 'History of the service catalog in the order it was recorded (AUTO CDC SCD type 2): __START_AT and __END_AT are Postgres commit times.';

CREATE FLOW services_history_cdc AS AUTO CDC INTO services_history
FROM STREAM(services_changes)
KEYS (service)
SEQUENCE BY committed_at
COLUMNS * EXCEPT (op, lsn, committed_at, debezium_at, processed_at)
STORED AS SCD TYPE 2;
