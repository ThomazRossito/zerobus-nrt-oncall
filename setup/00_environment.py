#!/usr/bin/env python3
# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
"""Phase 0 · checks the zerobus-nrt-oncall environment and runs the minimal AUTO CDC bitemporal test.

Usage, from the project root with the conda env active (conda activate zerobus-nrt-oncall):
  python setup/00_environment.py check                        read-only: Mac, Postgres, CLI, warehouse, clock,
                                                              catalog, service principal and Zerobus
  python setup/00_environment.py create                       creates the missing catalog and schemas and grants
                                                              USE CATALOG and USE SCHEMA to the service principal
  python setup/00_environment.py bitemporal                   deploys the test pipeline with the bundle, writes two
                                                              changes and checks the two postmortem answers
  python setup/00_environment.py bitemporal --channel PREVIEW the same, with the pipeline on the PREVIEW channel

Reads from .env: DATABRICKS_CONFIG_PROFILE, UC_CATALOG, WAREHOUSE_ID, ZEROBUS_SERVER_ENDPOINT, DATABRICKS_CLIENT_ID,
PG_HOST, PG_PORT, PG_USER_ADMIN and PG_PASSWORD_ADMIN. Never reads DATABRICKS_CLIENT_SECRET.
The output shows no host, e-mail, IDs or passwords, so it is safe to paste in a chat.
"""
from __future__ import annotations

import argparse
import json
import re
import socket
import sys
import time
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.common import (ROOT, SqlFailed, client, fail, first_line, guard, info, missing, ok,  # noqa: E402
                        read_env, run, save_local, scrub, section, sql, summary, warn)

SCHEMAS = {
    "otel": "OpenTelemetry signals written by Zerobus (OTLP)",
    "bronze": "Postgres changes as they arrived from Debezium through Zerobus",
    "silver": "Normalized changes and the AUTO CDC tables (SCD1, SCD2 and bitemporal)",
    "gold": "Per-service metrics by minute, errors by version and per-hop latency",
}
DATABRICKS = ("DATABRICKS_CONFIG_PROFILE", "UC_CATALOG", "WAREHOUSE_ID", "ZEROBUS_SERVER_ENDPOINT",
              "DATABRICKS_CLIENT_ID")
POSTGRES = ("PG_HOST", "PG_PORT", "PG_USER_ADMIN", "PG_PASSWORD_ADMIN")
PACKAGES = ("databricks-sdk", "psycopg", "opentelemetry-sdk", "opentelemetry-exporter-otlp-proto-grpc")
ZEROBUS_DOMAIN = {"azure": "azuredatabricks.net", "aws": "cloud.databricks.com"}

# bitemporal test: the postmortem question, which has two answers
PIPELINE = "phase0_bitemporal"                    # resource key in the bundle
SOURCE = "bronze.phase0_bitemporal_source"
TARGET = "silver.phase0_bitemporal"
CHANGES = (
    ("2.4.0", "2026-11-20 13:00:00", "2026-11-20 13:00:00"),   # went live at 13:00, recorded right away
    ("2.4.1", "2026-11-20 14:02:00", "2026-11-20 14:10:00"),   # went live at 14:02, recorded only at 14:10
)
MOMENT = "2026-11-20 14:05:00"                     # when the checkout error rate went up
Q_LIVE = """SELECT version FROM {target}
WHERE __START_AT <= TIMESTAMP'{t}' AND (__END_AT > TIMESTAMP'{t}' OR __END_AT IS NULL)
  AND __SYSTEM_END_AT IS NULL"""
Q_KNOWN = """SELECT version FROM {target}
WHERE __START_AT <= TIMESTAMP'{t}' AND (__END_AT > TIMESTAMP'{t}' OR __END_AT IS NULL)
  AND __SYSTEM_START_AT <= TIMESTAMP'{t}' AND (__SYSTEM_END_AT > TIMESTAMP'{t}' OR __SYSTEM_END_AT IS NULL)"""


# ---- check ------------------------------------------------------------------------------------------------

def check_mac() -> None:
    section("Mac")
    v = sys.version_info
    (ok if v >= (3, 11) else fail)(f"Python {v.major}.{v.minor}.{v.micro}")
    for package in PACKAGES:
        try:
            ok(f"{package} {metadata.version(package)}")
        except metadata.PackageNotFoundError:
            fail(f"{package} not installed: conda env update -f environment.yml")
    rc, out, _ = run(["databricks", "--version"])
    if rc == 0:
        ok(f"Databricks CLI {first_line(out).split()[-1]}")
    else:
        fail("Databricks CLI not found")
    rc, out, err = run(["java", "-version"])
    text = first_line(err or out)                 # java prints its version to stderr
    version = re.search(r'version "(\d+)', text)
    if rc == 0 and version and int(version.group(1)) >= 21:
        ok(f"Java {version.group(1)} (Debezium Server needs 21 or later)")
    elif rc == 0:
        fail(f"Java older than 21 ({text}): brew install openjdk@21")
    else:
        fail("Java not found: brew install openjdk@21")
    rc, out, _ = run(["otelcol-contrib", "--version"])
    if rc == 0:
        ok(f"OTel Collector: {first_line(out)}")
    else:
        fail("otelcol-contrib not found: darwin_arm64 release at https://opentelemetry.io/docs/collector/installation/")


def check_postgres(env: dict[str, str]) -> None:
    section("Local Postgres")
    if keys := missing(env, POSTGRES):
        fail("missing in .env: " + ", ".join(keys))
        return
    try:
        import psycopg
    except ImportError:
        fail("psycopg not installed")
        return
    try:
        with psycopg.connect(host=env["PG_HOST"], port=int(env["PG_PORT"]), user=env["PG_USER_ADMIN"],
                             password=env["PG_PASSWORD_ADMIN"], dbname="postgres", connect_timeout=5) as conn:
            value = {name: str(conn.execute(f"SHOW {name}").fetchone()[0])
                     for name in ("server_version", "wal_level", "max_replication_slots", "max_wal_senders")}
    except Exception as error:  # noqa: BLE001
        fail("could not connect as PG_USER_ADMIN: "
             + scrub(first_line(error)).replace(env["PG_USER_ADMIN"], "<PG_USER_ADMIN>"))
        return
    major = int(re.match(r"\d+", value["server_version"]).group())
    (ok if major >= 14 else fail)(f"Postgres {value['server_version'].split()[0]}")
    if value["wal_level"] == "logical":
        ok("wal_level = logical")
    else:
        fail(f"wal_level = {value['wal_level']}: set wal_level = logical in postgresql.conf and restart Postgres")
    for name in ("max_replication_slots", "max_wal_senders"):
        (ok if int(value[name]) >= 1 else fail)(f"{name} = {value[name]}")


def check_databricks(env: dict[str, str]):
    section("Databricks")
    if keys := missing(env, DATABRICKS):
        fail("missing in .env: " + ", ".join(keys))
        return None
    profile = env["DATABRICKS_CONFIG_PROFILE"]
    w = client(env)
    try:
        w.current_user.me()
    except Exception as error:  # noqa: BLE001
        fail(f"profile {profile}: {scrub(first_line(error))} (databricks auth login -p {profile})")
        return None
    ok(f"profile {profile}: authenticated")
    return w


def check_warehouse(w, env: dict[str, str]) -> bool:
    section("SQL warehouse and clock")
    try:
        wh = w.warehouses.get(env["WAREHOUSE_ID"])
    except Exception as error:  # noqa: BLE001
        fail(f"WAREHOUSE_ID: {scrub(first_line(error))}")
        return False
    state = wh.state.value if wh.state else "?"
    if wh.enable_serverless_compute:
        ok(f"serverless warehouse, size {wh.cluster_size}, state {state}")
    else:
        warn(f"warehouse is not serverless (size {wh.cluster_size}, state {state}): the app and Genie wake up slowly")
    samples = []
    try:
        for _ in range(5):                       # the 1st one may include the warehouse start; keep the fastest
            before = time.time()
            rows = sql(w, env["WAREHOUSE_ID"], "SELECT unix_micros(current_timestamp())")
            after = time.time()
            samples.append((after - before, int(rows[0][0]) / 1e6 - (before + after) / 2))
    except SqlFailed as error:
        fail(f"SQL on the warehouse: {error}")
        return False
    round_trip, offset = min(samples)
    ok(f"clock: Databricks is {offset * 1000:+.0f} ms from the Mac (uncertainty ±{round_trip * 500:.0f} ms)")
    save_local("phase0_clock.json", {"measured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                     "databricks_minus_mac_ms": round(offset * 1000),
                                     "uncertainty_ms": round(round_trip * 500)})
    return True


def check_catalog(w, env: dict[str, str]) -> bool:
    from databricks.sdk.errors import NotFound
    section("Unity Catalog")
    catalog = env["UC_CATALOG"]
    try:
        w.catalogs.get(catalog)
        ok(f"catalog {catalog}")
    except NotFound:
        fail(f"catalog {catalog} does not exist: python setup/00_environment.py create")
        return False
    except Exception as error:  # noqa: BLE001
        fail(f"catalog {catalog}: {scrub(first_line(error))}")
        return False
    complete = True
    for name in SCHEMAS:
        try:
            w.schemas.get(f"{catalog}.{name}")
            ok(f"schema {catalog}.{name}")
        except NotFound:
            fail(f"schema {catalog}.{name} does not exist: python setup/00_environment.py create")
            complete = False
        except Exception as error:  # noqa: BLE001
            fail(f"schema {catalog}.{name}: {scrub(first_line(error))}")
            complete = False
    return complete


def check_service_principal(w, env: dict[str, str], schemas_ok: bool) -> None:
    section("Zerobus service principal")
    client_id = env["DATABRICKS_CLIENT_ID"]
    try:
        found = list(w.service_principals.list(filter=f'applicationId eq "{client_id}"'))
    except Exception as error:  # noqa: BLE001
        fail(f"could not list the service principals: {scrub(first_line(error))}")
        return
    if not found:
        fail("DATABRICKS_CLIENT_ID does not belong to any service principal in this workspace")
        return
    active = found[0].active is not False
    (ok if active else fail)("service principal found" + ("" if active else ", but inactive"))
    if not schemas_ok:
        info("grants are checked once the catalog and the schemas exist")
        return
    catalog = env["UC_CATALOG"]
    for privilege, kind, name in (("USE CATALOG", "CATALOG", catalog), ("USE SCHEMA", "SCHEMA", f"{catalog}.otel"),
                                  ("USE SCHEMA", "SCHEMA", f"{catalog}.bronze")):
        try:
            rows = sql(w, env["WAREHOUSE_ID"], f"SHOW GRANTS `{client_id}` ON {kind} {name}")
        except SqlFailed as error:
            fail(f"{privilege} on {name}: could not read the grants ({error})")
            continue
        granted = {str(cell).upper().replace("_", " ") for row in rows for cell in row if cell}
        if privilege in granted:
            ok(f"{privilege} on {name}")
        elif "ALL PRIVILEGES" in granted:
            fail(f"{privilege} on {name}: only ALL PRIVILEGES, which Zerobus does not accept; run create")
        else:
            fail(f"{privilege} on {name} is missing: python setup/00_environment.py create")


def workspace_id(w) -> str | None:
    try:
        return str(w.get_workspace_id())
    except Exception:  # noqa: BLE001
        match = re.match(r"https://adb-(\d+)\.", w.config.host or "")
        return match.group(1) if match else None


def check_zerobus(w, env: dict[str, str]) -> None:
    section("Zerobus")
    try:
        metastore = w.metastores.summary()
        cloud, region = (metastore.cloud or "").lower(), (metastore.region or "").lower()
    except Exception as error:  # noqa: BLE001
        fail(f"could not read the metastore cloud and region: {scrub(first_line(error))}")
        return
    ok(f"cloud {cloud or '?'}, region {region or '?'}")
    given = re.sub(r"^https?://", "", env["ZEROBUS_SERVER_ENDPOINT"]).split("/")[0].split(":")[0]
    domain, wid = ZEROBUS_DOMAIN.get(cloud), workspace_id(w)
    if domain and wid:
        if given == f"{wid}.zerobus.{region}.{domain}":
            ok("ZEROBUS_SERVER_ENDPOINT in .env matches the workspace and the region")
        else:
            fail(f"ZEROBUS_SERVER_ENDPOINT does not match: here the format is <workspace-id>.zerobus.{region}.{domain}")
    else:
        warn("could not build the expected endpoint to compare: check the format in the Zerobus docs")
    start = time.monotonic()
    try:
        with socket.create_connection((given, 443), timeout=8):
            ok(f"Zerobus endpoint reachable on port 443 in {(time.monotonic() - start) * 1000:.0f} ms")
    except OSError as error:
        fail(f"Zerobus endpoint not reachable on port 443 ({type(error).__name__})")


def check(env: dict[str, str]) -> int:
    check_mac()
    check_postgres(env)
    w = check_databricks(env)
    if w:
        warehouse_ok = check_warehouse(w, env)
        schemas_ok = check_catalog(w, env) and warehouse_ok
        check_service_principal(w, env, schemas_ok)
        check_zerobus(w, env)
    return summary()


# ---- create -----------------------------------------------------------------------------------------------

def create(env: dict[str, str]) -> int:
    section("Databricks")
    if keys := missing(env, DATABRICKS):
        fail("missing in .env: " + ", ".join(keys))
        return summary()
    w, wh = client(env), env["WAREHOUSE_ID"]
    catalog, client_id = env["UC_CATALOG"], env["DATABRICKS_CLIENT_ID"]
    section("Unity Catalog")
    try:
        sql(w, wh, f"CREATE CATALOG IF NOT EXISTS `{catalog}` "
                   "COMMENT 'zerobus-nrt-oncall: near-real-time data, from commit to screen'")
        ok(f"catalog {catalog}")
    except SqlFailed as error:
        fail(f"catalog {catalog}: {error}")
        info("if the error asks for a MANAGED LOCATION, create the catalog in Catalog Explorer and run create again")
        return summary()
    for name, comment in SCHEMAS.items():
        try:
            sql(w, wh, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{name}` COMMENT '{comment}'")
            ok(f"schema {catalog}.{name}")
        except SqlFailed as error:
            fail(f"schema {catalog}.{name}: {error}")
    section("Service principal grants")
    for privilege, securable, label in (("USE CATALOG", f"CATALOG `{catalog}`", catalog),
                                        ("USE SCHEMA", f"SCHEMA `{catalog}`.`otel`", f"{catalog}.otel"),
                                        ("USE SCHEMA", f"SCHEMA `{catalog}`.`bronze`", f"{catalog}.bronze")):
        try:
            sql(w, wh, f"GRANT {privilege} ON {securable} TO `{client_id}`")
            ok(f"{privilege} on {label}")
        except SqlFailed as error:
            fail(f"{privilege} on {label}: {error}")
    info("per-table SELECT and MODIFY come in Phase 2, once the Zerobus tables exist")
    return summary()


# ---- bitemporal -------------------------------------------------------------------------------------------

def error_line(text: str) -> str:
    for line in (text or "").splitlines():
        if line.strip().lower().startswith("error"):
            return line.strip()
    return first_line(text)


def pipeline_error(w, pipeline_id: str) -> str:
    try:
        events = []
        for event in w.pipelines.list_pipeline_events(pipeline_id=pipeline_id, filter="level='ERROR'", max_results=10):
            events.append(event)
            if len(events) >= 10:
                break
    except Exception as error:  # noqa: BLE001
        return f"could not read the events ({scrub(first_line(error))})"
    for event in events:                         # the cause is usually in the exception, not in the event message
        if event.error and event.error.exceptions and event.error.exceptions[0].message:
            return scrub(first_line(event.error.exceptions[0].message))
    for event in events:
        if event.message:
            return scrub(first_line(event.message))
    return "no error message in the events"


def run_update(w, pipeline_id: str, from_scratch: bool) -> bool:
    try:
        update_id = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=from_scratch).update_id
    except Exception as error:  # noqa: BLE001
        fail(f"could not start the update: {scrub(first_line(error))}")
        return False
    start, previous, state = time.monotonic(), None, "?"
    while True:
        current = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id).update.state
        state = current.value if current else "?"
        if state != previous:
            info(f"{time.monotonic() - start:4.0f} s  {state}")
            previous = state
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            break
        if time.monotonic() - start > 1200:
            fail("the update took longer than 20 min")
            return False
        time.sleep(5)
    if state == "COMPLETED":
        ok(f"update completed in {time.monotonic() - start:.0f} s")
        return True
    fail(f"update {state}: {pipeline_error(w, pipeline_id)}")
    return False


def beta_hint(channel: str) -> None:
    info("if the error says the feature is not enabled or the syntax is unknown:")
    if channel != "PREVIEW":
        info("  1. run it again on the PREVIEW channel: python setup/00_environment.py bitemporal --channel PREVIEW")
    info("  2. in the workspace, open Previews and search for bitemporal or AUTO CDC")


def clock_time(value) -> str:
    if value is None:
        return "open"
    match = re.search(r"\d{2}:\d{2}", str(value))
    return match.group() if match else str(value)


def bitemporal(env: dict[str, str], channel: str) -> int:
    section("Databricks")
    if keys := missing(env, DATABRICKS):
        fail("missing in .env: " + ", ".join(keys))
        return summary()
    w, wh, catalog, profile = client(env), env["WAREHOUSE_ID"], env["UC_CATALOG"], env["DATABRICKS_CONFIG_PROFILE"]
    source, target = f"{catalog}.{SOURCE}", f"{catalog}.{TARGET}"
    variables = ["--var", f"catalog={catalog}", "--var", f"channel={channel}"]

    section(f"Bundle (channel {channel})")
    rc, out, err = run(["databricks", "bundle", "deploy", *variables, "-p", profile], cwd=ROOT / "bundle", timeout=900)
    if rc != 0:
        fail(f"bundle deploy: {scrub(error_line(err or out))}")
        return summary()
    ok("bundle deploy")
    rc, out, err = run(["databricks", "bundle", "summary", "-o", "json", *variables, "-p", profile], cwd=ROOT / "bundle")
    try:
        pipeline_id = json.loads(out)["resources"]["pipelines"][PIPELINE]["id"]
    except (ValueError, KeyError, TypeError):
        fail(f"pipeline not found in the bundle summary: {scrub(error_line(err or out))}")
        return summary()
    ok("test pipeline in the workspace")

    section("Test source")
    values = "('checkout', '{}', TIMESTAMP'{}', TIMESTAMP'{}')".format
    try:
        sql(w, wh, f"CREATE OR REPLACE TABLE {source} (service STRING, version STRING, deployed_at TIMESTAMP, "
                   "recorded_at TIMESTAMP) COMMENT 'Phase 0: source of the AUTO CDC bitemporal test'")
        sql(w, wh, f"INSERT INTO {source} VALUES {values(*CHANGES[0])}")
    except SqlFailed as error:
        fail(f"test source: {error}")
        return summary()
    ok("1st change: 2.4.0 went live at 13:00 and was recorded at 13:00")

    section("Pipeline, 1st update (from scratch)")
    if not run_update(w, pipeline_id, from_scratch=True):
        beta_hint(channel)
        return summary()
    try:
        sql(w, wh, f"INSERT INTO {source} VALUES {values(*CHANGES[1])}")
    except SqlFailed as error:
        fail(f"2nd change: {error}")
        return summary()
    ok("2nd change: 2.4.1 went live at 14:02, but was only recorded at 14:10")

    section("Pipeline, 2nd update")
    if not run_update(w, pipeline_id, from_scratch=False):
        beta_hint(channel)
        return summary()

    section("What the bitemporal table kept")
    try:
        rows = sql(w, wh, f"SELECT version, __START_AT, __END_AT, __SYSTEM_START_AT, __SYSTEM_END_AT FROM {target} "
                          "ORDER BY __SYSTEM_START_AT, __START_AT")
        live = sorted({row[0] for row in sql(w, wh, Q_LIVE.format(target=target, t=MOMENT))})
        known = sorted({row[0] for row in sql(w, wh, Q_KNOWN.format(target=target, t=MOMENT))})
    except SqlFailed as error:
        fail(f"query on the bitemporal table: {error}")
        return summary()
    print("         version  live from  until      known from  until")
    for version, start, end, sys_start, sys_end in rows:
        print(f"         {version:<8} {clock_time(start):<10} {clock_time(end):<10} {clock_time(sys_start):<11} "
              f"{clock_time(sys_end)}")

    section("The two postmortem questions, at 14:05")
    (ok if live == ["2.4.1"] else fail)(f"version that was live: {', '.join(live) or 'none'} (expected 2.4.1)")
    (ok if known == ["2.4.0"] else fail)(f"what the catalog said at the time: {', '.join(known) or 'none'} "
                                         "(expected 2.4.0)")
    return summary()


def main() -> int:
    parser = argparse.ArgumentParser(description="zerobus-nrt-oncall · Phase 0")
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("check", help="read-only environment check")
    actions.add_parser("create", help="creates the catalog and schemas and grants the service principal")
    test = actions.add_parser("bitemporal", help="minimal AUTO CDC bitemporal test")
    test.add_argument("--channel", choices=("CURRENT", "PREVIEW"), default="CURRENT")
    args = parser.parse_args()
    env = read_env()
    if args.action == "check":
        return check(env)
    if args.action == "create":
        return create(env)
    return bitemporal(env, args.channel)


if __name__ == "__main__":
    sys.exit(guard(main))
