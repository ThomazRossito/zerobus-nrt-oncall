-- ------------------------------------------------------------------------------
-- Author:       Thomaz Antonio Rossito Neto
--               Principal Data Architect & AI Strategy
--               Databricks Solutions Architect Champion
-- Linkedin:     in/thomaz-antonio-rossito-neto/
-- Created:      2026-11-20
--
-- -----------------------------------------------------------------------------
-- Phase 0 · minimal AUTO CDC bitemporal test: confirms the feature (Beta) runs in this workspace.
--
-- Two changes in the service catalog: checkout 2.4.0 went live at 13:00 and was recorded right away;
-- 2.4.1 went live at 14:02 but was only recorded at 14:10. The table keeps both timelines:
--   business time (when a version was live)        -> __START_AT, __END_AT
--   system time   (when the catalog learned of it) -> __SYSTEM_START_AT, __SYSTEM_END_AT
--
-- The source rows are written by: python setup/00_environment.py bitemporal
-- bronze.phase0_bitemporal_source is read from the pipeline's default catalog (var.catalog in the bundle).

CREATE OR REFRESH STREAMING TABLE phase0_bitemporal
COMMENT 'Phase 0: live version (business time) and when the catalog learned about it (system time)';

CREATE FLOW phase0_bitemporal_cdc AS AUTO CDC INTO phase0_bitemporal
FROM STREAM(bronze.phase0_bitemporal_source)
KEYS (service)
SEQUENCE BY deployed_at           -- business time
SYSTEM SEQUENCE BY recorded_at    -- system time
STORED AS BITEMPORAL;
