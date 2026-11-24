#!/usr/bin/env python3
# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
"""Phase 0 · checks the zerobus-nrt-oncall environment and creates the catalog and schemas.

Usage, from the project root with the conda env active (conda activate zerobus-nrt-oncall):
  python setup/00_environment.py check                        read-only: Mac, Postgres, CLI, warehouse, clock,
                                                              catalog, service principal and Zerobus
  python setup/00_environment.py create                       creates the missing catalog and schemas and grants
                                                              USE CATALOG and USE SCHEMA to the service principal

Reads from .env: DATABRICKS_CONFIG_PROFILE, UC_CATALOG, WAREHOUSE_ID, ZEROBUS_SERVER_ENDPOINT, DATABRICKS_CLIENT_ID,
PG_HOST, PG_PORT, PG_USER_ADMIN and PG_PASSWORD_ADMIN. Never reads DATABRICKS_CLIENT_SECRET.
The output shows no host, e-mail, IDs or passwords, so it is safe to paste in a chat.
"""
from __future__ import annotations

import argparse
import re
import socket
import sys
import time
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.common import (SqlFailed, client, fail, first_line, guard, info, missing, ok, read_env,  # noqa: E402
                        run, save_local, scrub, section, sql, summary, warn)

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


def main() -> int:
    parser = argparse.ArgumentParser(description="zerobus-nrt-oncall · Phase 0")
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("check", help="read-only environment check")
    actions.add_parser("create", help="creates the catalog and schemas and grants the service principal")
    args = parser.parse_args()
    env = read_env()
    if args.action == "check":
        return check(env)
    return create(env)


if __name__ == "__main__":
    sys.exit(guard(main))
