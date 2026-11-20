-- ------------------------------------------------------------------------------
-- Author:       Thomaz Antonio Rossito Neto
--               Principal Data Architect & AI Strategy
--               Databricks Solutions Architect Champion
-- Linkedin:     in/thomaz-antonio-rossito-neto/
-- Created:      2026-11-20
--
-- -----------------------------------------------------------------------------
-- Phase 1 · the shop's service catalog and incidents: the source of the CDC side (Postgres -> Debezium -> Zerobus).
-- Run by "python setup/01_source.py create". Idempotent: it only creates what is missing.

CREATE SCHEMA IF NOT EXISTS cmdb;

-- One row per service: the version the catalog says is live and when it went live (business time).
-- The catalog may learn about a deploy late; the commit time of the change is the system time.
CREATE TABLE IF NOT EXISTS cmdb.services (
    service      text        PRIMARY KEY,
    version      text        NOT NULL,
    deployed_at  timestamptz NOT NULL,
    owner        text        NOT NULL
);

-- Incidents opened by the alerting bot. A duplicate gets deleted, so CDC sees inserts, updates and deletes.
-- suspected_version is the version the on-call engineer blamed, read from the catalog at that moment.
CREATE TABLE IF NOT EXISTS cmdb.incidents (
    incident_id        bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    service            text        NOT NULL,
    severity           text        NOT NULL CHECK (severity IN ('sev1', 'sev2', 'sev3')),
    status             text        NOT NULL CHECK (status IN ('open', 'investigating', 'mitigated', 'resolved')),
    title              text        NOT NULL,
    suspected_version  text,
    opened_at          timestamptz NOT NULL,
    updated_at         timestamptz NOT NULL
);

-- Alert history, only for the large initial load of the Debezium test (Phase 2). It does not go to silver.
CREATE TABLE IF NOT EXISTS cmdb.alerts_history (
    alert_id  bigint           PRIMARY KEY,
    service   text             NOT NULL,
    rule      text             NOT NULL,
    severity  text             NOT NULL,
    fired_at  timestamptz      NOT NULL,
    value     double precision NOT NULL
);
