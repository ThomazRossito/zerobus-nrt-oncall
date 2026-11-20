#!/usr/bin/env python3
# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
"""Phase 1 · the local Postgres of the CDC side: database, cmdb schema, tables, replication user and publication.

Usage, from the project root with the conda env active (conda activate zerobus-nrt-oncall):
  python setup/01_source.py check     read-only: database, tables, service catalog, replication user, publication
  python setup/01_source.py create    creates what is missing and seeds the service catalog (checkout 2.4.0)
  python setup/01_source.py alerts    loads cmdb.alerts_history (10 million rows) for the Debezium test of Phase 2
  python setup/01_source.py reset     puts the catalog back in the initial state after an interrupted run
  python setup/01_source.py drop-slot NAME   drops an inactive replication slot, which holds WAL until dropped

Reads from .env: PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD, PG_USER_ADMIN and PG_PASSWORD_ADMIN.
The output shows no host, user names or passwords, so it is safe to paste in a chat.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.common import (ROOT, fail, guard, info, missing, ok, pg_connect, pg_error, read_env,  # noqa: E402
                        section, summary, warn)
from sim.scenario import PUBLICATION, SCHEMA, SERVICES, SLOT  # noqa: E402

POSTGRES = ("PG_HOST", "PG_PORT", "PG_DBNAME", "PG_USER", "PG_PASSWORD", "PG_USER_ADMIN", "PG_PASSWORD_ADMIN")
DDL = ROOT / "sql" / "phase1" / "cmdb.sql"
COLUMNS = {
    "services": ["service", "version", "deployed_at", "owner"],
    "incidents": ["incident_id", "service", "severity", "status", "title", "suspected_version", "opened_at",
                  "updated_at"],
    "alerts_history": ["alert_id", "service", "rule", "severity", "fired_at", "value"],
}
ALERT_ROWS = 10_000_000
# deterministic rows, no random(): the same load gives the same table
ALERTS_SQL = """INSERT INTO cmdb.alerts_history (alert_id, service, rule, severity, fired_at, value)
SELECT i,
       (ARRAY['checkout', 'payment', 'inventory'])[1 + mod(i, 3)::int],
       (ARRAY['error_rate_high', 'latency_p95_high', 'saturation_high', 'pod_restarts'])[1 + mod(i / 3, 4)::int],
       (ARRAY['sev3', 'sev3', 'sev2', 'sev1'])[1 + mod(i / 12, 4)::int],
       TIMESTAMPTZ '2026-01-01 00:00:00+00' + i * INTERVAL '2 seconds',
       mod(i * 7919, 10000) / 100.0
FROM generate_series(1::bigint, %s::bigint) AS i"""


def _connect_or_fail(env: dict[str, str], dbname: str | None = None):
    try:
        return pg_connect(env, dbname=dbname)
    except Exception as error:  # noqa: BLE001
        fail(f"could not connect as PG_USER_ADMIN: {pg_error(error, env)}")
        return None


def _database_exists(conn, name: str) -> bool:
    return conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone() is not None


def _tables(conn) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for table, column in conn.execute(
            "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = %s "
            "ORDER BY table_name, ordinal_position", (SCHEMA,)):
        found.setdefault(table, []).append(column)
    return found


def _published(conn) -> set[str] | None:
    if conn.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (PUBLICATION,)).fetchone() is None:
        return None
    return {row[0] for row in conn.execute(
        "SELECT tablename FROM pg_publication_tables WHERE pubname = %s AND schemaname = %s", (PUBLICATION, SCHEMA))}


# ---- check ------------------------------------------------------------------------------------------------

def check(env: dict[str, str]) -> int:
    section("Local Postgres")
    if keys := missing(env, POSTGRES):
        fail("missing in .env: " + ", ".join(keys))
        return summary()
    database = env["PG_DBNAME"]
    if (conn := _connect_or_fail(env, "postgres")) is None:
        return summary()
    with conn:
        settings = {name: str(conn.execute(f"SHOW {name}").fetchone()[0])
                    for name in ("wal_level", "max_replication_slots")}
        slots = conn.execute(
            "SELECT slot_name, plugin, database, active, "
            "pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) "
            "FROM pg_replication_slots ORDER BY slot_name").fetchall()
        exists = _database_exists(conn, database)
    (ok if settings["wal_level"] == "logical" else fail)(f"wal_level = {settings['wal_level']}")
    info(f"replication slots in use: {len(slots)} of {settings['max_replication_slots']}")
    for name, plugin, slot_database, active, retained in slots:
        where = f"{plugin}, database {slot_database}" if plugin else "physical"
        held = retained or "0 bytes"
        if active:
            info(f"  slot {name} ({where}): active, holds {held} of WAL")
        elif name == SLOT:
            info(f"  slot {name} ({where}): the project's Debezium slot, inactive while Debezium is stopped; "
                 f"holds {held} of WAL")
        else:
            warn(f"slot {name} ({where}): inactive, holds {held} of WAL and keeps holding it until someone reads "
                 f"or drops it: python setup/01_source.py drop-slot {name}")
    if not exists:
        fail(f"database {database} does not exist: python setup/01_source.py create")
        return summary()
    ok(f"database {database}")
    if (conn := _connect_or_fail(env)) is None:
        return summary()
    with conn:
        section(f"Schema {SCHEMA}")
        found, complete = _tables(conn), True
        for table, columns in COLUMNS.items():
            if table not in found:
                fail(f"table {SCHEMA}.{table} does not exist: python setup/01_source.py create")
                complete = False
            elif found[table] != columns:
                fail(f"table {SCHEMA}.{table} has other columns ({', '.join(found[table])}): drop it and run create")
                complete = False
            else:
                ok(f"table {SCHEMA}.{table}")
        if complete:
            check_contents(conn)
        section("Replication")
        check_publication(conn)
        check_replication_user(conn, env, complete)
    return summary()


def check_contents(conn) -> None:
    section("Service catalog and incidents")
    catalog = dict(conn.execute("SELECT service, version FROM cmdb.services").fetchall())
    for service, spec in SERVICES.items():
        version = catalog.get(service)
        if version is None:
            fail(f"{service} is missing from cmdb.services: python setup/01_source.py create")
        elif version == spec["version"]:
            ok(f"{service} {version} (initial state)")
        else:
            warn(f"{service} is at {version}, not {spec['version']}: python setup/01_source.py reset")
    total, still_open = conn.execute(
        "SELECT count(*), count(*) FILTER (WHERE status <> 'resolved') FROM cmdb.incidents").fetchone()
    (ok if still_open == 0 else warn)(f"incidents: {total}, still open: {still_open}"
                                      + ("" if still_open == 0 else " (reset resolves them)"))
    estimate = conn.execute(
        "SELECT reltuples::bigint FROM pg_class WHERE oid = 'cmdb.alerts_history'::regclass").fetchone()[0]
    if estimate >= ALERT_ROWS * 0.95:
        ok(f"alerts_history: about {estimate:,} rows")
    else:
        info(f"alerts_history: about {max(estimate, 0):,} rows; before the Debezium test of Phase 2: "
             "python setup/01_source.py alerts")


def check_publication(conn) -> None:
    published = _published(conn)
    if published is None:
        fail(f"publication {PUBLICATION} does not exist: python setup/01_source.py create")
        return
    absent = [table for table in COLUMNS if table not in published]
    if absent:
        fail(f"publication {PUBLICATION} without {', '.join(absent)}: python setup/01_source.py create")
    else:
        ok(f"publication {PUBLICATION} with {', '.join(COLUMNS)}")


def check_replication_user(conn, env: dict[str, str], tables_ok: bool) -> None:
    role = conn.execute("SELECT rolcanlogin, rolreplication FROM pg_roles WHERE rolname = %s",
                        (env["PG_USER"],)).fetchone()
    if role is None:
        fail("replication user (PG_USER) does not exist: python setup/01_source.py create")
        return
    if all(role):
        ok("replication user (PG_USER): LOGIN and REPLICATION")
    else:
        fail("replication user (PG_USER) lacks LOGIN or REPLICATION: python setup/01_source.py create")
    if tables_ok:
        denied = [table for table in COLUMNS if not conn.execute(
            "SELECT has_table_privilege(%s, %s, 'SELECT')", (env["PG_USER"], f"{SCHEMA}.{table}")).fetchone()[0]]
        if denied:
            fail(f"PG_USER cannot SELECT {', '.join(denied)}: python setup/01_source.py create")
        else:
            ok(f"SELECT for PG_USER on the {len(COLUMNS)} tables")
    try:
        with pg_connect(env, replication_user=True) as user_conn:
            user_conn.execute("SELECT 1")
        ok("PG_USER logs in with PG_PASSWORD")
    except Exception as error:  # noqa: BLE001
        fail(f"PG_USER could not log in: {pg_error(error, env)}")


# ---- create -----------------------------------------------------------------------------------------------

def create(env: dict[str, str]) -> int:
    from psycopg import sql
    section("Local Postgres")
    if keys := missing(env, POSTGRES):
        fail("missing in .env: " + ", ".join(keys))
        return summary()
    database, user = env["PG_DBNAME"], env["PG_USER"]
    if (conn := _connect_or_fail(env, "postgres")) is None:
        return summary()
    with conn:
        if _database_exists(conn, database):
            ok(f"database {database} (exists)")
        else:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
            ok(f"database {database} created")
    if (conn := _connect_or_fail(env)) is None:
        return summary()
    with conn:
        section(f"Schema {SCHEMA}")
        conn.execute(DDL.read_text(encoding="utf-8"))
        ok(f"schema {SCHEMA} with services, incidents and alerts_history ({DDL.relative_to(ROOT)})")
        inserted = 0
        for service, spec in SERVICES.items():
            inserted += conn.execute(
                "INSERT INTO cmdb.services (service, version, deployed_at, owner) VALUES (%s, %s, now(), %s) "
                "ON CONFLICT (service) DO NOTHING", (service, spec["version"], spec["owner"])).rowcount
        ok(f"service catalog: {inserted} row(s) inserted, {len(SERVICES) - inserted} already there")

        section("Replication")
        role = conn.execute("SELECT rolcanlogin, rolreplication FROM pg_roles WHERE rolname = %s", (user,)).fetchone()
        if role is None:
            conn.execute(sql.SQL("CREATE ROLE {} WITH LOGIN REPLICATION PASSWORD {}").format(
                sql.Identifier(user), sql.Literal(env["PG_PASSWORD"])))
            ok("replication user (PG_USER) created with LOGIN and REPLICATION")
        elif not all(role):
            conn.execute(sql.SQL("ALTER ROLE {} WITH LOGIN REPLICATION").format(sql.Identifier(user)))
            ok("replication user (PG_USER) now has LOGIN and REPLICATION; password kept")
        else:
            ok("replication user (PG_USER) exists; password kept")
        for statement in ("GRANT CONNECT ON DATABASE {database} TO {user}", "GRANT USAGE ON SCHEMA {schema} TO {user}",
                          "GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {user}"):
            conn.execute(sql.SQL(statement).format(database=sql.Identifier(database), schema=sql.Identifier(SCHEMA),
                                                   user=sql.Identifier(user)))
        ok("CONNECT, USAGE and SELECT on the cmdb tables for PG_USER")

        published = _published(conn)
        if published is None:
            tables = sql.SQL(", ").join(sql.Identifier(SCHEMA, table) for table in COLUMNS)
            conn.execute(sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(sql.Identifier(PUBLICATION), tables))
            ok(f"publication {PUBLICATION} created with {', '.join(COLUMNS)}")
        elif absent := [table for table in COLUMNS if table not in published]:
            tables = sql.SQL(", ").join(sql.Identifier(SCHEMA, table) for table in absent)
            conn.execute(sql.SQL("ALTER PUBLICATION {} ADD TABLE {}").format(sql.Identifier(PUBLICATION), tables))
            ok(f"publication {PUBLICATION}: added {', '.join(absent)}")
        else:
            ok(f"publication {PUBLICATION} (exists, with the three tables)")
    info("the replication slot comes in Phase 2, when Debezium starts for the first time")
    return summary()


# ---- alerts and reset -------------------------------------------------------------------------------------

def alerts(env: dict[str, str], rows: int) -> int:
    section("Alert history for the Debezium test")
    if keys := missing(env, POSTGRES):
        fail("missing in .env: " + ", ".join(keys))
        return summary()
    if (conn := _connect_or_fail(env)) is None:
        return summary()
    with conn:
        if conn.execute("SELECT EXISTS (SELECT 1 FROM cmdb.alerts_history)").fetchone()[0]:
            current = conn.execute("SELECT count(*) FROM cmdb.alerts_history").fetchone()[0]
            ok(f"alerts_history already has {current:,} rows: nothing to do")
            return summary()
        info(f"loading {rows:,} rows; with 10 million this takes a few minutes")
        start = time.monotonic()
        conn.execute(ALERTS_SQL, (rows,))
        conn.execute("ANALYZE cmdb.alerts_history")
        size = conn.execute("SELECT pg_size_pretty(pg_total_relation_size('cmdb.alerts_history'))").fetchone()[0]
        ok(f"alerts_history: {rows:,} rows in {time.monotonic() - start:.0f} s ({size} with the primary key)")
    return summary()


def reset(env: dict[str, str]) -> int:
    section("Service catalog")
    if keys := missing(env, POSTGRES):
        fail("missing in .env: " + ", ".join(keys))
        return summary()
    if (conn := _connect_or_fail(env)) is None:
        return summary()
    with conn:
        catalog = dict(conn.execute("SELECT service, version FROM cmdb.services").fetchall())
        changed = 0
        for service, spec in SERVICES.items():
            if catalog.get(service) == spec["version"]:
                continue
            conn.execute("INSERT INTO cmdb.services (service, version, deployed_at, owner) VALUES (%s, %s, now(), %s) "
                         "ON CONFLICT (service) DO UPDATE SET version = EXCLUDED.version, deployed_at = now()",
                         (service, spec["version"], spec["owner"]))
            ok(f"{service}: {catalog.get(service) or 'missing'} -> {spec['version']}")
            changed += 1
        if not changed:
            ok("catalog already in the initial state")
        section("Incidents")
        resolved = conn.execute("UPDATE cmdb.incidents SET status = 'resolved', updated_at = now() "
                                "WHERE status <> 'resolved'").rowcount
        ok(f"{resolved} incident(s) left open by an interrupted run marked resolved" if resolved
           else "no incident left open")
    return summary()


def drop_slot(env: dict[str, str], name: str) -> int:
    section("Replication slot")
    if keys := missing(env, POSTGRES):
        fail("missing in .env: " + ", ".join(keys))
        return summary()
    if (conn := _connect_or_fail(env, "postgres")) is None:
        return summary()
    with conn:
        row = conn.execute(
            "SELECT database, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) "
            "FROM pg_replication_slots WHERE slot_name = %s", (name,)).fetchone()
    if row is None:
        fail(f"slot {name} does not exist")
        return summary()
    slot_database, active, retained = row
    if active:
        fail(f"slot {name} is active: stop whatever reads from it first")
        return summary()
    # a logical slot is dropped from a connection to its own database, which works on every Postgres version
    if (conn := _connect_or_fail(env, slot_database or "postgres")) is None:
        return summary()
    with conn:
        conn.execute("SELECT pg_drop_replication_slot(%s)", (name,))
    ok(f"slot {name} dropped; {retained or '0 bytes'} of WAL no longer held by it")
    return summary()


def main() -> int:
    parser = argparse.ArgumentParser(description="zerobus-nrt-oncall · Phase 1 source")
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("check", help="read-only check of the local Postgres")
    actions.add_parser("create", help="creates the database, tables, replication user and publication")
    load = actions.add_parser("alerts", help="loads cmdb.alerts_history for the Debezium test")
    load.add_argument("--rows", type=int, default=ALERT_ROWS, help=f"rows to load (default {ALERT_ROWS:,})")
    actions.add_parser("reset", help="puts the service catalog back in the initial state")
    drop = actions.add_parser("drop-slot", help="drops an inactive replication slot")
    drop.add_argument("name", help="slot name, as check lists it")
    args = parser.parse_args()
    env = read_env()
    if args.action == "check":
        return check(env)
    if args.action == "create":
        return create(env)
    if args.action == "alerts":
        return alerts(env, args.rows)
    if args.action == "drop-slot":
        return drop_slot(env, args.name)
    return reset(env)


if __name__ == "__main__":
    sys.exit(guard(main))
