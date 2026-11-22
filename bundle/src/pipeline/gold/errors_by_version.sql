-- ------------------------------------------------------------------------------
-- Author:       Thomaz Antonio Rossito Neto
--               Principal Data Architect & AI Strategy
--               Databricks Solutions Architect Champion
-- Linkedin:     in/thomaz-antonio-rossito-neto/
-- Created:      2026-11-22
--
-- -----------------------------------------------------------------------------
-- Phase 3 · gold: the telemetry next to the catalog. For each row of gold.service_health: the version the spans
-- carry, the version the catalog now says was live at that moment (business time, today's knowledge) and the version
-- the catalog named at that same moment (business and system time both at that moment: the bitemporal as-of).
-- The moment (as_of) is the middle of the row's spans, so a bucket that straddles a deploy is judged on each side of
-- it, away from the deploy instant itself: there the first request and deployed_at carry the same instant, each
-- rounded on its own, and the first run of Phase 3, which looked up at the first span, put one bucket of each
-- deploy on the wrong side.
-- From 14:02 to 14:10 the spans carry 2.4.1 while the catalog still named 2.4.0: catalog_was_right is false there.

CREATE OR REFRESH MATERIALIZED VIEW gold.errors_by_version
COMMENT 'Each 5-second bucket of telemetry next to the catalog, looked up at as_of (the middle of the spans of that bucket and version): the version the spans carry, the version the catalog now says was live, and the version the catalog named at that moment (bitemporal as-of). catalog_was_right is false where they differ.'
AS
WITH h AS (
  SELECT *, timestamp_micros((unix_micros(first_span_at) + unix_micros(last_span_at)) DIV 2) AS as_of
  FROM gold.service_health
)
SELECT
  h.bucket_start,
  h.service,
  h.version                 AS telemetry_version,
  h.requests,
  h.errors,
  h.error_rate,
  h.as_of,
  live.version              AS catalog_live_version,
  known.version             AS catalog_known_version,
  h.version = known.version AS catalog_was_right
FROM h
LEFT JOIN silver.services_bitemporal AS live
  ON  live.service = h.service
  AND live.__START_AT <= h.as_of
  AND (live.__END_AT > h.as_of OR live.__END_AT IS NULL)
  AND live.__SYSTEM_END_AT IS NULL
LEFT JOIN silver.services_bitemporal AS known
  ON  known.service = h.service
  AND known.__START_AT <= h.as_of
  AND (known.__END_AT > h.as_of OR known.__END_AT IS NULL)
  AND known.__SYSTEM_START_AT <= h.as_of
  AND (known.__SYSTEM_END_AT > h.as_of OR known.__SYSTEM_END_AT IS NULL);
