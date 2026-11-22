-- ------------------------------------------------------------------------------
-- Author:       Thomaz Antonio Rossito Neto
--               Principal Data Architect & AI Strategy
--               Databricks Solutions Architect Champion
-- Linkedin:     in/thomaz-antonio-rossito-neto/
-- Created:      2026-11-22
--
-- -----------------------------------------------------------------------------
-- Phase 3 · gold: the shop's requests per 5-second bucket, service and version, from the OTel spans Zerobus writes
-- (one SERVER span per request). The version is the service.version resource attribute, so each row shows the version
-- that served the traffic, whatever the catalog said. At the default speed 12, a bucket is one minute of the scenario.
-- As in "Query OpenTelemetry data" (Databricks docs): attributes with dots are read as attributes:['key'], a span
-- lasts end_time_unix_nano - start_time_unix_nano, and enums keep their OTLP names (SPAN_KIND_SERVER,
-- STATUS_CODE_ERROR). first_span_at and last_span_at bound the spans of each row; errors_by_version looks the catalog
-- up at their middle.

CREATE OR REFRESH MATERIALIZED VIEW gold.service_health
COMMENT 'Requests, errors and latency (p50, p95) per 5-second bucket, service and version, from the OTel spans. The version is the one the spans carry (service.version). At the default speed 12, one bucket is one minute of the scenario.'
AS SELECT
  timestamp_seconds(unix_seconds(time) DIV 5 * 5)                                      AS bucket_start,
  timestamp_seconds(unix_seconds(time) DIV 5 * 5 + 5)                                  AS bucket_end,
  service_name                                                                         AS service,
  resource.attributes:['service.version']::string                                      AS version,
  count(*)                                                                             AS requests,
  count_if(status.code = 'STATUS_CODE_ERROR')                                          AS errors,
  round(count_if(status.code = 'STATUS_CODE_ERROR') / count(*), 4)                     AS error_rate,
  round(percentile_approx((end_time_unix_nano - start_time_unix_nano) / 1e6, 0.5), 1)  AS p50_ms,
  round(percentile_approx((end_time_unix_nano - start_time_unix_nano) / 1e6, 0.95), 1) AS p95_ms,
  min(time)                                                                            AS first_span_at,
  max(time)                                                                            AS last_span_at
FROM otel.shop_otel_spans
WHERE kind = 'SPAN_KIND_SERVER'
GROUP BY ALL;
