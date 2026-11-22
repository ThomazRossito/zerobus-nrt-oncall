#!/usr/bin/env python3
# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-22
#
# -----------------------------------------------------------------------------
"""Phase 3 · the on-call pipeline: deploys it with the bundle, runs an update and checks what it produced.

Usage, from the project root with the conda env active (conda activate zerobus-nrt-oncall):
  python setup/03_pipeline.py check                 read-only: what the pipeline reads (bronze and the OTel spans)
                                                    and, once it ran, what it produced: the deletes in SCD1, the
                                                    SCD2 history, the bitemporal table, the two postmortem answers
                                                    and the gold tables
  python setup/03_pipeline.py run                   checks the inputs, deploys the bundle, runs one update of the
                                                    pipeline and checks what it produced
  python setup/03_pipeline.py run --full-refresh    the same, with every table recomputed from bronze

The pipeline is bundle/resources/oncall.pipeline.yml, with its SQL in bundle/src/pipeline/. Phase 3 closes when the
as-of queries answer 2.4.1 for "which version was live when the incident opened" and 2.4.0 for "what the catalog said
at that moment", and the deleted incident is not in silver.incidents_current.
Times are shown on the scenario clock (14:00 to 14:20) of the latest run of the sim: "before" is earlier than that
run (the initial snapshot) and "open" is a period with no end yet.
Reads from .env: DATABRICKS_CONFIG_PROFILE, UC_CATALOG and WAREHOUSE_ID. Never reads DATABRICKS_CLIENT_SECRET: it
removes it and DATABRICKS_CLIENT_ID from this process, unread, so the CLI and the SDK use the profile even in a
terminal that sourced .env. The output shows no host, e-mail or IDs, so it is safe to paste in a chat.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.bundle import deploy, pipeline_id, run_update  # noqa: E402
from lib.common import (COUNTS, ROOT, SqlFailed, client, fail, first_line, guard, info, missing, ok,  # noqa: E402
                        read_env, scrub, section, sql, summary, warn)
from sim.scenario import BAD_RELEASE, DEFAULT_SPEED, DURATION_S, SERVICES, TIMELINE, clock  # noqa: E402

NEEDED = ("DATABRICKS_CONFIG_PROFILE", "UC_CATALOG", "WAREHOUSE_ID")
# exported for Debezium by "set -a; source .env": the CLI and the SDK read them too and would mix them with the profile
SERVICE_PRINCIPAL = ("DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET")
PIPELINE = "oncall"                    # resource key in bundle/resources/oncall.pipeline.yml
DIGESTS = ROOT / ".local" / "phase1" / "digests.jsonl"
TABLES = ("silver.services_changes", "silver.incidents_changes", "silver.incidents_current",
          "silver.services_history", "silver.services_bitemporal", "gold.service_health", "gold.errors_by_version",
          "gold.incident_postmortem", "gold.hop_latency")
BUCKET_S = 5                           # the bucket of gold.service_health

# bronze: per table, the condition that marks a change silver cannot convert. A delete of an incident carries the key
# only; the catalog never deletes a service, so every services row needs its deployed_at.
UNREADABLE = {
    "services": "deployed_at IS NULL OR try_cast(deployed_at AS TIMESTAMP) IS NULL",
    "incidents": "__op <> 'd' AND (try_cast(opened_at AS TIMESTAMP) IS NULL "
                 "OR try_cast(updated_at AS TIMESTAMP) IS NULL)",
}

# the scenario (sim/scenario.py): the release that broke checkout, and the incident of the postmortem
SERVICE, BAD_VERSION = BAD_RELEASE                                                          # checkout, 2.4.1
GOOD_VERSION = SERVICES[SERVICE]["version"]                                                 # 2.4.0
DEPLOY_S = next(s for s, action, d in TIMELINE if action == "deploy" and not d["record"])   # 14:02, catalog not told
RECORD_S = next(s for s, action, d in TIMELINE if action == "catalog_record")               # 14:10, the late record
DELETE_S = next(s for s, action, _ in TIMELINE if action == "incident_delete")              # 14:07, the duplicate
OPEN_S, TITLE = next((s, d["title"]) for s, action, d in TIMELINE
                     if action == "incident_open" and d["key"] == "main")                  # 14:05

# the two postmortem questions at the moment t: business time at t with today's knowledge, then both times at t
Q_LIVE = """SELECT version FROM silver.services_bitemporal
WHERE service = {service} AND __START_AT <= {t} AND (__END_AT > {t} OR __END_AT IS NULL)
  AND __SYSTEM_END_AT IS NULL"""
Q_KNOWN = """SELECT version FROM silver.services_bitemporal
WHERE service = {service} AND __START_AT <= {t} AND (__END_AT > {t} OR __END_AT IS NULL)
  AND __SYSTEM_START_AT <= {t} AND (__SYSTEM_END_AT > {t} OR __SYSTEM_END_AT IS NULL)"""


def literal(text: str) -> str:
    """A SQL string literal for a value that comes from sim/scenario.py."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def number(value) -> int:
    return int(value) if value is not None else 0


def duration(ms: int) -> str:
    seconds = ms / 1000
    if seconds < 1:
        return f"{ms} ms"
    if seconds < 120:
        return f"{seconds:.1f} s"
    if seconds < 7200:
        return f"{seconds / 60:.0f} min"
    return f"{seconds / 3600:,.0f} h"


def run_speed() -> float:
    """Speed of the latest run of the sim: sim/run.py writes one line per run to .local/phase1/digests.jsonl."""
    try:
        return float(json.loads(DIGESTS.read_text(encoding="utf-8").splitlines()[-1])["speed"])
    except (OSError, ValueError, KeyError, IndexError):
        return DEFAULT_SPEED


class Query:
    """Runs statements on the SQL warehouse, with the project catalog as the default catalog."""

    def __init__(self, w, env: dict[str, str]):
        self.w, self.warehouse, self.catalog = w, env["WAREHOUSE_ID"], env["UC_CATALOG"]

    def __call__(self, statement: str) -> list[list]:
        return sql(self.w, self.warehouse, statement, catalog=self.catalog)


class ScenarioClock:
    """Shows the real instants of the latest run of the sim on the scenario clock, 14:00 to 14:20.

    The anchor is the incident of the postmortem: the sim opens it at scenario second OPEN_S and writes that real
    instant to opened_at. The speed comes from the latest line of .local/phase1/digests.jsonl. Without the incident,
    times are shown in UTC.
    """

    def __init__(self, opened_ms: int | None):
        self.speed = run_speed()
        self.t0 = None if opened_ms is None else opened_ms / 1000 - OPEN_S / self.speed

    def __call__(self, ms) -> str:
        if ms is None:
            return "open"
        if self.t0 is None:
            return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%H:%M:%S UTC")
        second = (int(ms) / 1000 - self.t0) * self.speed
        if second < 0:
            return "before"
        if second > DURATION_S + 60:
            return "after"
        return clock(round(second))

    def minutes(self, seconds) -> str:
        """A real duration in seconds, in minutes of the scenario clock."""
        return "-" if seconds is None else f"{int(seconds) * self.speed / 60:.0f} min"


def connect(env: dict[str, str]):
    section("Databricks")
    if keys := missing(env, NEEDED):
        fail("missing in .env: " + ", ".join(keys))
        return None
    for name in SERVICE_PRINCIPAL:
        os.environ.pop(name, None)
    try:
        w = client(env)
        w.current_user.me()
    except Exception as error:  # noqa: BLE001
        fail(f"profile {env['DATABRICKS_CONFIG_PROFILE']}: {scrub(first_line(error))}")
        return None
    ok(f"profile {env['DATABRICKS_CONFIG_PROFILE']}: authenticated")
    return w


# ---- inputs ------------------------------------------------------------------------------------------------

def inputs(q: Query) -> bool:
    """What the pipeline reads: the CDC in bronze and the spans in otel. False when something would stop it."""
    failures = COUNTS["fail"]
    section("Bronze: the changes silver reads")
    for table, unreadable in UNREADABLE.items():
        try:
            rows = q(f"SELECT __op, count(*), count_if(__lsn IS NULL OR __source_ts_ms IS NULL OR __ts_ms IS NULL), "
                     f"count_if({unreadable}) FROM bronze.{table} GROUP BY __op ORDER BY __op")
        except SqlFailed as error:
            fail(f"bronze.{table}: {error}")
            continue
        by_op = {op: number(count) for op, count, _, _ in rows}
        total = sum(by_op.values())
        detail = ", ".join(f"{op} {count:,}" for op, count in by_op.items())
        info(f"bronze.{table}: {total:,} rows" + (f" ({detail})" if total else ""))
        if not total:
            fail(f"bronze.{table} is empty: Phase 2 leaves one run of the sim there")
            continue
        if lacking := sum(number(row[2]) for row in rows):
            fail(f"bronze.{table}: {lacking:,} change(s) without __lsn, __source_ts_ms or __ts_ms; "
                 "silver stops on them")
        else:
            ok(f"bronze.{table}: every change has its WAL position, its commit time and Debezium's time")
        if bad := sum(number(row[3]) for row in rows):
            fail(f"bronze.{table}: {bad:,} change(s) with a time that does not convert to TIMESTAMP")
        else:
            ok(f"bronze.{table}: the times convert to TIMESTAMP")
        if table == "services" and by_op.get("d"):
            fail(f"bronze.services: {by_op['d']} delete(s); the catalog never deletes a service in this experiment, "
                 "and silver.services_changes stops the update on one")

    section("OTel spans: what gold reads")
    try:
        rows = q("SELECT service_name, resource.attributes:['service.version']::string, count(*), "
                 "count_if(status.code = 'STATUS_CODE_ERROR') FROM otel.shop_otel_spans "
                 "WHERE kind = 'SPAN_KIND_SERVER' GROUP BY 1, 2 ORDER BY 1, 2")
        if not rows:
            kinds = [str(row[0]) for row in q("SELECT DISTINCT kind FROM otel.shop_otel_spans LIMIT 10")]
            fail("no span with kind SPAN_KIND_SERVER in otel.shop_otel_spans"
                 + (f"; kinds found: {', '.join(kinds)}" if kinds else ": run python sim/run.py --otlp"))
            return COUNTS["fail"] == failures
        for service, version, spans, errors in rows:
            info(f"{service} {version or '(no service.version)'}: {number(spans):,} requests, "
                 f"{number(errors):,} errors")
        bad = next((row for row in rows if row[0] == SERVICE and row[1] == BAD_VERSION), None)
        if bad is None:
            fail(f"no span of {SERVICE} {BAD_VERSION}: a run of python sim/run.py --otlp has to reach the OTel tables")
        elif not number(bad[3]):
            codes = [str(row[0]) for row in q("SELECT DISTINCT status.code FROM otel.shop_otel_spans LIMIT 10")]
            fail(f"{SERVICE} {BAD_VERSION} has no span with status.code STATUS_CODE_ERROR; codes found: "
                 + ", ".join(codes))
        else:
            ok("the spans carry service.version and STATUS_CODE_ERROR, which gold.service_health counts")
    except SqlFailed as error:
        fail(f"otel.shop_otel_spans: {error}")
    return COUNTS["fail"] == failures


# ---- results -----------------------------------------------------------------------------------------------

def deletes(q: Query, at: ScenarioClock) -> None:
    section("SCD1, silver.incidents_current: the deletes")
    rows = q("""SELECT d.incident_id, c.title, unix_millis(c.created_at), unix_millis(d.deleted_at),
       cur.incident_id IS NOT NULL
FROM (SELECT incident_id, max(committed_at) AS deleted_at FROM silver.incidents_changes WHERE op = 'd'
      GROUP BY incident_id) AS d
LEFT JOIN (SELECT incident_id, max_by(title, lsn) AS title, min(committed_at) AS created_at
           FROM silver.incidents_changes WHERE op IN ('c', 'r') GROUP BY incident_id) AS c
  ON c.incident_id = d.incident_id
LEFT JOIN silver.incidents_current AS cur ON cur.incident_id = d.incident_id
ORDER BY d.incident_id""")
    if not rows:
        fail(f"no delete in silver.incidents_changes: the scenario deletes the duplicate incident at {clock(DELETE_S)}")
    for incident_id, title, created_ms, deleted_ms, still in rows:
        line = f'incident #{incident_id} "{title or "?"}", created {at(created_ms)} and deleted {at(deleted_ms)}'
        if str(still).lower() == "true":
            fail(f"{line}: still in incidents_current")
        else:
            ok(f"{line}: not in incidents_current")
    current, seen, deleted = (number(value) for value in q(
        "SELECT (SELECT count(*) FROM silver.incidents_current), "
        "(SELECT count(DISTINCT incident_id) FROM silver.incidents_changes), "
        "(SELECT count(DISTINCT incident_id) FROM silver.incidents_changes WHERE op = 'd')")[0])
    (ok if current == seen - deleted else fail)(
        f"incidents_current: {current} incident(s), the {seen} seen in the changes minus the {deleted} deleted")


def history(q: Query, at: ScenarioClock) -> None:
    section(f"SCD2, silver.services_history: {SERVICE} in the order the catalog recorded it")
    rows = q(f"SELECT version, unix_millis(deployed_at), unix_millis(__START_AT), unix_millis(__END_AT) "
             f"FROM silver.services_history WHERE service = {literal(SERVICE)} ORDER BY __START_AT")
    if not rows:
        fail(f"no row of {SERVICE} in silver.services_history")
        return
    if len(rows) > 6:
        info(f"the last 6 of {len(rows)} rows")
    print("         version  went live  recorded from  until")
    for version, deployed_ms, start_ms, end_ms in rows[-6:]:
        print(f"         {(version or '-'):<8} {at(deployed_ms):<10} {at(start_ms):<14} {at(end_ms)}")
    late = [row for row in rows if row[0] == BAD_VERSION]
    if not late:
        fail(f"no {BAD_VERSION} in silver.services_history: the catalog records it at {clock(RECORD_S)}")
    elif at(late[-1][2]) == clock(RECORD_S):
        ok(f"SCD2 starts {BAD_VERSION} at {clock(RECORD_S)}, when the catalog recorded it: its periods follow the "
           f"commits, and {clock(DEPLOY_S)} stays only in the deployed_at column")
    else:
        warn(f"SCD2 starts {BAD_VERSION} at {at(late[-1][2])}; expected {clock(RECORD_S)}, "
             "the commit of the late record")


def bitemporal(q: Query, at: ScenarioClock) -> None:
    section(f"Bitemporal, silver.services_bitemporal: {SERVICE} on both timelines")
    rows = q(f"SELECT version, unix_millis(__START_AT), unix_millis(__END_AT), unix_millis(__SYSTEM_START_AT), "
             f"unix_millis(__SYSTEM_END_AT) FROM silver.services_bitemporal WHERE service = {literal(SERVICE)} "
             "ORDER BY __SYSTEM_START_AT, __START_AT")
    if not rows:
        fail(f"no row of {SERVICE} in silver.services_bitemporal")
        return
    if len(rows) > 8:
        info(f"the last 8 of {len(rows)} rows")
    print("         version  live from  until      known from  until")
    for version, start, end, system_start, system_end in rows[-8:]:
        print(f"         {(version or '-'):<8} {at(start):<10} {at(end):<10} {at(system_start):<11} {at(system_end)}")
    ok(f"{len(rows)} row(s): what was live (live from, until) and since when the catalog said so (known from, until)")


def answers(q: Query, at: ScenarioClock, incident_id: int, opened_ms: int) -> None:
    section(f"The two postmortem questions, at {at(opened_ms)}, when incident #{incident_id} opened")
    moment = f"timestamp_millis({opened_ms})"
    live = sorted({str(row[0]) for row in q(Q_LIVE.format(service=literal(SERVICE), t=moment))})
    known = sorted({str(row[0]) for row in q(Q_KNOWN.format(service=literal(SERVICE), t=moment))})
    (ok if live == [BAD_VERSION] else fail)(
        f"version that was live: {', '.join(live) or 'none'} (expected {BAD_VERSION})")
    (ok if known == [GOOD_VERSION] else fail)(
        f"what the catalog said at the time: {', '.join(known) or 'none'} (expected {GOOD_VERSION})")


def gold(q: Query, at: ScenarioClock, incident_id: int) -> None:
    section("Gold")
    rows = q(f"SELECT version, count(*), sum(requests), sum(errors) FROM gold.service_health "
             f"WHERE service = {literal(SERVICE)} GROUP BY version ORDER BY version")
    for version, buckets, requests, errors in rows:
        rate = number(errors) / number(requests) if number(requests) else 0.0
        info(f"gold.service_health, {SERVICE} {version}: {number(buckets):,} buckets, {number(requests):,} requests, "
             f"{number(errors):,} errors ({rate:.1%})")
    bad = next((row for row in rows if row[0] == BAD_VERSION), None)
    if bad and number(bad[3]):
        ok(f"gold.service_health: the errors of {SERVICE} {BAD_VERSION}, by the version the spans carry")
    else:
        fail(f"gold.service_health has no error of {SERVICE} {BAD_VERSION}")

    rows = q(f"SELECT telemetry_version, catalog_known_version, catalog_live_version, count(*), "
             f"unix_millis(min(bucket_start)) FROM gold.errors_by_version WHERE service = {literal(SERVICE)} "
             "AND NOT catalog_was_right GROUP BY 1, 2, 3 ORDER BY 5")
    per_run = round((RECORD_S - DEPLOY_S) / at.speed / BUCKET_S)
    found = False
    for telemetry, known, live, buckets, first_ms in rows:
        if (telemetry, known, live) == (BAD_VERSION, GOOD_VERSION, BAD_VERSION):
            found = True
            ok(f"gold.errors_by_version: {buckets} bucket(s) from {at(first_ms)} with the spans on {BAD_VERSION} and "
               f"the catalog still on {GOOD_VERSION} (about {per_run} per run)")
        else:
            info(f"gold.errors_by_version: {buckets} bucket(s) from {at(first_ms)} with the spans on {telemetry}, "
                 f"the catalog then on {known} and the catalog now on {live}")
    if not found:
        fail(f"gold.errors_by_version has no bucket with the spans on {BAD_VERSION} and the catalog on {GOOD_VERSION}")
    # the deploy instant on both sides: the first request of each version and its deployed_at in the catalog
    rows = q(f"""SELECT b.version, unix_millis(b.__START_AT), unix_micros(min(s.time)) - unix_micros(b.__START_AT)
FROM silver.services_bitemporal AS b
JOIN otel.shop_otel_spans AS s
  ON s.service_name = b.service AND s.resource.attributes:['service.version']::string = b.version
 AND s.time BETWEEN b.__START_AT - INTERVAL 1 SECOND AND b.__START_AT + INTERVAL 1 SECOND
WHERE b.service = {literal(SERVICE)} AND b.__SYSTEM_END_AT IS NULL AND s.kind = 'SPAN_KIND_SERVER'
GROUP BY b.version, b.__START_AT ORDER BY b.__START_AT""")
    for version, start_ms, delta_us in rows:
        delta = number(delta_us)
        side = f"{-delta} µs before" if delta < 0 else f"{delta} µs after" if delta > 0 else "on the same µs as"
        info(f"deploy of {version} at {at(start_ms)}: the first request of {version} starts {side} its deployed_at")

    rows = q(f"SELECT suspected_at_open, catalog_known_at_open, catalog_live_at_open, telemetry_version_at_open, "
             f"suspected_now, seconds_to_mitigate, seconds_to_resolve FROM gold.incident_postmortem "
             f"WHERE incident_id = {incident_id}")
    if not rows:
        fail(f"incident #{incident_id} is not in gold.incident_postmortem")
    else:
        labels = ("suspected at open", "catalog then", "catalog now", "spans", "suspected now")
        expected = (GOOD_VERSION, GOOD_VERSION, BAD_VERSION, BAD_VERSION, BAD_VERSION)
        got = tuple(rows[0][:5])
        listed = ", ".join(f"{label} {value or '-'}" for label, value in zip(labels, got))
        (ok if got == expected else fail)(f"gold.incident_postmortem, incident #{incident_id}: {listed}")
        if got != expected:
            info("expected: " + ", ".join(f"{label} {value}" for label, value in zip(labels, expected)))
        info(f"mitigated {at.minutes(rows[0][5])} and resolved {at.minutes(rows[0][6])} after it opened, "
             "on the scenario clock")

    rows = q("SELECT source_table, count(*), percentile_approx(capture_ms, 0.5), percentile_approx(capture_ms, 0.95), "
             "percentile_approx(to_silver_ms, 0.5) FROM gold.hop_latency GROUP BY 1 ORDER BY 1")
    if not rows:
        fail("gold.hop_latency is empty")
    for table, changes, p50, p95, to_silver in rows:
        ok(f"gold.hop_latency, {table}: {number(changes)} change(s), commit → Debezium p50 {duration(number(p50))}, "
           f"p95 {duration(number(p95))}; commit → silver p50 {duration(number(to_silver))}")
    if rows and max(number(row[2]) for row in rows) > 60_000:
        info("commit → Debezium above a minute: Debezium read these changes from the slot after the fact (it was "
             "stopped or behind), not as they were committed")
    if rows:
        info("commit → silver counts the wait until this update started: it measures the pipeline only when the "
             "pipeline runs continuous (Phase 6)")


def results(w, q: Query, env: dict[str, str]) -> None:
    """What the pipeline produced, checked against the scenario."""
    from databricks.sdk.errors import NotFound
    section("Pipeline tables")
    absent = []
    for name in TABLES:
        try:
            w.tables.get(f"{env['UC_CATALOG']}.{name}")
        except NotFound:
            absent.append(name)
        except Exception as error:  # noqa: BLE001
            fail(f"{name}: {scrub(first_line(error))}")
            return
    if len(absent) == len(TABLES):
        info("not created yet: python setup/03_pipeline.py run")
        return
    if absent:
        fail("missing: " + ", ".join(absent) + "; python setup/03_pipeline.py run")
        return
    try:
        counts = q("SELECT " + ", ".join(f"(SELECT count(*) FROM {name})" for name in TABLES))[0]
        incident = q(f"SELECT incident_id, unix_millis(opened_at) FROM silver.incidents_current "
                     f"WHERE service = {literal(SERVICE)} AND title = {literal(TITLE)} "
                     "ORDER BY incident_id DESC LIMIT 1")
    except SqlFailed as error:
        fail(f"pipeline tables: {error}")
        return
    for name, count in zip(TABLES, counts):
        info(f"{name}: {number(count):,} rows")
    incident_id, opened_ms = (number(incident[0][0]), number(incident[0][1])) if incident else (None, None)
    at = ScenarioClock(opened_ms)
    if incident:
        info(f'times on the scenario clock of the latest run (speed {at.speed:g}), placed by incident '
             f'#{incident_id} "{TITLE}", opened at {clock(OPEN_S)}')
    else:
        fail(f'no incident "{TITLE}" in silver.incidents_current; times in UTC')
    try:
        deletes(q, at)
        history(q, at)
        bitemporal(q, at)
        if incident:
            answers(q, at, incident_id, opened_ms)
            gold(q, at, incident_id)
    except SqlFailed as error:
        fail(f"query on the pipeline tables: {error}")


# ---- actions -----------------------------------------------------------------------------------------------

def check(env: dict[str, str]) -> int:
    w = connect(env)
    if w is None:
        return summary()
    q = Query(w, env)
    inputs(q)
    results(w, q, env)
    return summary()


def run_pipeline(env: dict[str, str], full_refresh: bool) -> int:
    w = connect(env)
    if w is None:
        return summary()
    q = Query(w, env)
    if not inputs(q):
        info("nothing was deployed: the inputs above have to pass first")
        return summary()
    section("Bundle")
    variables = {"catalog": env["UC_CATALOG"]}
    info("bundle deploy: about a minute")
    if not deploy(env, variables):
        return summary()
    found = pipeline_id(env, variables, PIPELINE)
    if found is None:
        return summary()
    ok("pipeline zerobus_nrt_oncall in the workspace")
    section("Pipeline update" + (", full refresh" if full_refresh else ""))
    if run_update(w, found, full_refresh):
        results(w, q, env)
    return summary()


def main() -> int:
    parser = argparse.ArgumentParser(description="zerobus-nrt-oncall · Phase 3")
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("check", help="read-only: what the pipeline reads and, once it ran, what it produced")
    update = actions.add_parser("run", help="deploys the bundle, runs one update and checks what it produced")
    update.add_argument("--full-refresh", action="store_true", help="recomputes every table from bronze")
    args = parser.parse_args()
    env = read_env()
    if args.action == "check":
        return check(env)
    return run_pipeline(env, args.full_refresh)


if __name__ == "__main__":
    sys.exit(guard(main))
