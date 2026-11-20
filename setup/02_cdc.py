#!/usr/bin/env python3
# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
"""Phase 2 · the CDC side: bronze tables in Databricks, grants for Zerobus and the state of Debezium on the Mac.

Usage, from the project root with the conda env active (conda activate zerobus-nrt-oncall):
  python setup/02_cdc.py check          read-only: bronze tables, grants, rows per change type, Debezium and its slot
  python setup/02_cdc.py create         creates the bronze tables and grants SELECT and MODIFY to the service principal
  python setup/02_cdc.py reset          shows what a new initial load needs; with --yes it does it: drops the slot,
                                        deletes the offsets and empties the bronze tables (Debezium must be stopped)

Debezium Server runs with ingest/debezium/run.sh, and the initial load in parallel with
ingest/debezium/run_parallel.py. Reads from .env: DATABRICKS_CONFIG_PROFILE, UC_CATALOG, WAREHOUSE_ID,
DATABRICKS_CLIENT_ID and the PG_* variables. Never reads DATABRICKS_CLIENT_SECRET.
The output shows no host, e-mail, IDs or passwords, so it is safe to paste in a chat.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.common import (ROOT, SqlFailed, client, fail, first_line, guard, info, missing, ok,  # noqa: E402
                        pg_connect, pg_error, read_env, scrub, section, sql, summary, warn)
from sim.scenario import SLOT  # noqa: E402

DATABRICKS = ("DATABRICKS_CONFIG_PROFILE", "UC_CATALOG", "WAREHOUSE_ID", "DATABRICKS_CLIENT_ID")
POSTGRES = ("PG_HOST", "PG_PORT", "PG_DBNAME", "PG_USER_ADMIN", "PG_PASSWORD_ADMIN")
DDL = ROOT / "sql" / "phase2" / "bronze.sql"
DEBEZIUM = ROOT / "ingest" / "debezium"
DIST = DEBEZIUM / "dist" / "debezium-server"
OFFSETS = DEBEZIUM / "data" / "offsets.dat"
PARALLEL = DEBEZIUM / "data" / "parallel"
CDC_COLUMNS = [("__op", "string"), ("__ts_ms", "bigint"), ("__source_ts_ms", "bigint"), ("__lsn", "bigint"),
               ("__deleted", "string")]
BRONZE = {
    "services": [("service", "string"), ("version", "string"), ("deployed_at", "string"), ("owner", "string")],
    "incidents": [("incident_id", "bigint"), ("service", "string"), ("severity", "string"), ("status", "string"),
                  ("title", "string"), ("suspected_version", "string"), ("opened_at", "string"),
                  ("updated_at", "string")],
    "alerts_history": [("alert_id", "bigint"), ("service", "string"), ("rule", "string"), ("severity", "string"),
                       ("fired_at", "string"), ("value", "double")],
}
# a snapshot (r) holds one row per key: a key seen twice means rows sent twice (a snapshot taken again after a
# failure, or a batch resent after a lost ack)
KEYS = {"services": "service", "incidents": "incident_id", "alerts_history": "alert_id"}


def statements(path: Path) -> list[str]:
    """The statements of a .sql file: comment lines out, each statement ends with a line that ends in ';'."""
    found, current = [], []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.lstrip().startswith("--") or not line.strip():
            continue
        current.append(line)
        if line.rstrip().endswith(";"):
            found.append("\n".join(current).rstrip().rstrip(";"))
            current = []
    return found


def _databricks(env: dict[str, str]):
    section("Databricks")
    if keys := missing(env, DATABRICKS):
        fail("missing in .env: " + ", ".join(keys))
        return None
    try:
        w = client(env)
        w.current_user.me()
    except Exception as error:  # noqa: BLE001
        fail(f"profile {env['DATABRICKS_CONFIG_PROFILE']}: {scrub(first_line(error))}")
        return None
    ok(f"profile {env['DATABRICKS_CONFIG_PROFILE']}: authenticated")
    return w


# ---- check ------------------------------------------------------------------------------------------------

def offsets_state() -> str:
    """none, snapshot (an initial snapshot under way or interrupted) or streaming, read from Debezium's offsets file.

    The file is a serialized Java map, but each offset is kept as JSON text, so the snapshot flag can be read as bytes.
    """
    if not OFFSETS.exists():
        return "none"
    return "snapshot" if b'"snapshot_completed":false' in OFFSETS.read_bytes() else "streaming"


def check(env: dict[str, str]) -> int:
    from databricks.sdk.errors import NotFound
    if (w := _databricks(env)) is None:
        return summary()
    warehouse, catalog = env["WAREHOUSE_ID"], env["UC_CATALOG"]

    section("Bronze tables")
    present = []
    for table, columns in BRONZE.items():
        expected = columns + CDC_COLUMNS
        try:
            table_info = w.tables.get(f"{catalog}.bronze.{table}")
        except NotFound:
            fail(f"bronze.{table} does not exist: python setup/02_cdc.py create")
            continue
        except Exception as error:  # noqa: BLE001
            fail(f"bronze.{table}: {scrub(first_line(error))}")
            continue
        ordered = sorted(table_info.columns or [], key=lambda column: column.position or 0)
        found = [(column.name, (column.type_text or "").lower()) for column in ordered]
        if found != expected:
            listed = ", ".join(f"{name} {kind}" for name, kind in found)
            fail(f"bronze.{table} has other columns ({listed}): drop it and run create")
            continue
        ok(f"bronze.{table}: {len(expected)} columns, timestamps as STRING")
        present.append(table)

    section("Zerobus grants for the service principal")
    client_id = env["DATABRICKS_CLIENT_ID"]
    for table in present:
        try:
            rows = sql(w, warehouse, f"SHOW GRANTS `{client_id}` ON TABLE bronze.{table}", catalog=catalog)
        except SqlFailed as error:
            fail(f"bronze.{table}: could not read the grants ({error})")
            continue
        granted = {str(cell).upper().replace("_", " ") for row in rows for cell in row if cell}
        lacking = [privilege for privilege in ("SELECT", "MODIFY") if privilege not in granted]
        if lacking:
            fail(f"bronze.{table} lacks {' and '.join(lacking)}: python setup/02_cdc.py create")
        else:
            ok(f"SELECT and MODIFY on bronze.{table}")

    section("Rows in bronze, by change type")
    for table in present:
        try:
            rows = sql(w, warehouse, f"SELECT __op, count(*), count_if(__deleted = 'true') FROM bronze.{table} "
                                     "GROUP BY __op ORDER BY __op", catalog=catalog)
        except SqlFailed as error:
            fail(f"bronze.{table}: {error}")
            continue
        total = sum(int(count) for _, count, _ in rows)
        detail = ", ".join(f"{op} {int(count):,}" for op, count, _ in rows)
        info(f"bronze.{table}: {total:,} rows" + (f" ({detail})" if detail else ""))
        # the unwrap SMT rewrites a delete as a row with __deleted = 'true'; every other change has 'false'
        wrong = [op for op, count, flagged in rows if int(flagged) != (int(count) if op == "d" else 0)]
        deletes = sum(int(count) for op, count, _ in rows if op == "d")
        if wrong:
            fail(f"bronze.{table}: __deleted does not match the change type for {', '.join(wrong)}")
        elif deletes:
            ok(f"bronze.{table}: {deletes:,} delete(s) with __deleted = 'true', no other row flagged")
        key = KEYS[table]
        try:
            reads, distinct, low, high = sql(
                w, warehouse, f"SELECT count_if(__op = 'r'), count(DISTINCT CASE WHEN __op = 'r' THEN {key} END), "
                              f"min(CASE WHEN __op = 'r' THEN {key} END), max(CASE WHEN __op = 'r' THEN {key} END) "
                              f"FROM bronze.{table}", catalog=catalog)[0]
        except SqlFailed as error:
            fail(f"bronze.{table}: {error}")
            continue
        if (repeated := int(reads) - int(distinct)) > 0:
            warn(f"bronze.{table}: {repeated:,} snapshot row(s) with a repeated {key}: rows sent twice")
        elif int(reads) and table == "alerts_history":
            ok(f"bronze.{table}: one snapshot row per {key}, from {int(low):,} to {int(high):,}")

    section("Debezium on the Mac")
    jars = sorted((DIST / "lib").glob("debezium-server-databricks-zerobus-*.jar")) if (DIST / "lib").exists() else []
    if (DIST / "run.sh").exists() and jars:
        ok(f"Debezium Server in ingest/debezium/dist, with {jars[0].name}")
    else:
        fail("Debezium Server with the Zerobus sink not found in ingest/debezium/dist: build it from the tag")
    (ok if (DEBEZIUM / "application.properties").exists() else fail)("ingest/debezium/application.properties")
    snapshot = offsets_state()
    info("offsets: " + {"none": "none yet: the next run.sh takes the initial snapshot (after run_parallel.py, "
                                "start the CDC with DBZ_SNAPSHOT_MODE=no_data)",
                        "snapshot": "initial snapshot under way, or stopped before the end (a restart takes it again)",
                        "streaming": "ingest/debezium/data/offsets.dat, Debezium resumes from there"}[snapshot])
    runs = sorted(path for path in PARALLEL.iterdir() if path.is_dir()) if PARALLEL.exists() else []
    if runs:
        count = len([path for path in runs[-1].iterdir() if path.name.startswith("i")])
        info(f"parallel load: newest run in ingest/debezium/data/parallel/{runs[-1].name}, {count} instance(s)")
    if keys := missing(env, POSTGRES):
        fail("missing in .env: " + ", ".join(keys))
        return summary()
    try:
        with pg_connect(env, dbname="postgres") as conn:
            slot = conn.execute(
                "SELECT active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) "
                "FROM pg_replication_slots WHERE slot_name = %s", (SLOT,)).fetchone()
    except Exception as error:  # noqa: BLE001
        fail(f"could not connect as PG_USER_ADMIN: {pg_error(error, env)}")
        return summary()
    if slot is None:
        info(f"slot {SLOT}: not created yet (Debezium creates it on the first start)")
    else:
        # Debezium acquires the slot only when it starts streaming, so it shows inactive during the initial snapshot
        state = ("active, Debezium is streaming" if slot[0] else
                 "inactive, Debezium is stopped" if snapshot == "streaming" else
                 "inactive: Debezium is stopped, or still taking the initial snapshot")
        info(f"slot {SLOT}: {state}; holds {slot[1] or '0 bytes'} of WAL")
    return summary()


# ---- create -----------------------------------------------------------------------------------------------

def create(env: dict[str, str]) -> int:
    if (w := _databricks(env)) is None:
        return summary()
    warehouse, catalog, client_id = env["WAREHOUSE_ID"], env["UC_CATALOG"], env["DATABRICKS_CLIENT_ID"]
    section("Bronze tables")
    for statement in statements(DDL):
        name = statement.split("(")[0].split()[-1]
        try:
            sql(w, warehouse, statement, catalog=catalog)
            ok(f"{name} ({DDL.relative_to(ROOT)})")
        except SqlFailed as error:
            fail(f"{name}: {error}")
    section("Zerobus grants for the service principal")
    for table in BRONZE:
        try:
            sql(w, warehouse, f"GRANT SELECT, MODIFY ON TABLE bronze.{table} TO `{client_id}`", catalog=catalog)
            ok(f"SELECT and MODIFY on bronze.{table}")
        except SqlFailed as error:
            fail(f"bronze.{table}: {error}")
    return summary()


# ---- reset ------------------------------------------------------------------------------------------------

def reset(env: dict[str, str], yes: bool) -> int:
    section("New initial load")
    if not yes:
        info(f"with --yes: drops the replication slot {SLOT} (Debezium must be stopped), deletes")
        info("ingest/debezium/data/offsets.dat and empties the three bronze tables")
        info("nothing done")
        return summary()
    if keys := missing(env, POSTGRES):
        fail("missing in .env: " + ", ".join(keys))
        return summary()
    if (w := _databricks(env)) is None:         # check Databricks first: nothing is dropped if it fails
        return summary()
    section("Local Postgres and Debezium state")
    try:
        with pg_connect(env, dbname="postgres") as conn:
            slot = conn.execute("SELECT active, database FROM pg_replication_slots WHERE slot_name = %s",
                                (SLOT,)).fetchone()
        if slot and slot[0]:
            fail(f"slot {SLOT} is active: stop Debezium (Ctrl+C in its terminal) and run again")
            return summary()
        if slot:
            with pg_connect(env, dbname=slot[1] or "postgres") as conn:
                conn.execute("SELECT pg_drop_replication_slot(%s)", (SLOT,))
            ok(f"slot {SLOT} dropped")
        else:
            ok(f"slot {SLOT}: none to drop")
    except Exception as error:  # noqa: BLE001
        fail(f"local Postgres: {pg_error(error, env)}")
        return summary()
    if OFFSETS.exists():
        OFFSETS.unlink()
        ok("offsets deleted")
    else:
        ok("offsets: none to delete")
    section("Bronze tables")
    for table in BRONZE:
        try:
            sql(w, env["WAREHOUSE_ID"], f"TRUNCATE TABLE bronze.{table}", catalog=env["UC_CATALOG"])
            ok(f"bronze.{table} emptied")
        except SqlFailed as error:
            fail(f"bronze.{table}: {error}")
    info("next: ingest/debezium/run.sh takes a new initial snapshot, or ingest/debezium/run_parallel.py takes it")
    info("in parallel")
    return summary()


def main() -> int:
    parser = argparse.ArgumentParser(description="zerobus-nrt-oncall · Phase 2 CDC")
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("check", help="read-only check of bronze, grants and Debezium")
    actions.add_parser("create", help="creates the bronze tables and the Zerobus grants")
    again = actions.add_parser("reset", help="prepares a new initial load")
    again.add_argument("--yes", action="store_true", help="really drop the slot, the offsets and the bronze rows")
    args = parser.parse_args()
    env = read_env()
    if args.action == "check":
        return check(env)
    if args.action == "create":
        return create(env)
    return reset(env, args.yes)


if __name__ == "__main__":
    sys.exit(guard(main))
