#!/usr/bin/env python3
# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
"""Phase 2 · the OpenTelemetry side: the three OTel tables in Databricks, their Zerobus grants and what arrived.

Usage, from the project root with the conda env active (conda activate zerobus-nrt-oncall):
  python setup/02_otel.py check     read-only: the tables and their schema version, the grants, the rows per signal,
                                    whether the last run of the sim with --otlp reached them whole, the Collector port
  python setup/02_otel.py create    creates the three tables (sql/phase2/otel.sql) and grants SELECT and MODIFY on each
                                    to the service principal (USE CATALOG and USE SCHEMA on otel came in Phase 0)

The OTel Collector runs with ingest/otel/run.sh, and the sim sends to it with python sim/run.py --otlp. Reads from
.env: DATABRICKS_CONFIG_PROFILE, UC_CATALOG, WAREHOUSE_ID and DATABRICKS_CLIENT_ID. Never reads
DATABRICKS_CLIENT_SECRET. The output shows no host, e-mail or IDs, so it is safe to paste in a chat.
"""
from __future__ import annotations

import argparse
import re
import socket
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.common import (ROOT, SqlFailed, client, fail, first_line, guard, info, missing, ok, read_env,  # noqa: E402
                        scrub, section, sql, statements, summary, warn)

DATABRICKS = ("DATABRICKS_CONFIG_PROFILE", "UC_CATALOG", "WAREHOUSE_ID", "DATABRICKS_CLIENT_ID")
DDL = ROOT / "sql" / "phase2" / "otel.sql"
RUNS = ROOT / ".local" / "phase1"
COLLECTOR = ("127.0.0.1", 4317)
SCHEMA_VERSION = "v2"
# signal -> table and the column with each record's own time in ns, which places it inside a run of the sim
SIGNALS = {"spans": ("otel.shop_otel_spans", "start_time_unix_nano"),
           "logs": ("otel.shop_otel_logs", "time_unix_nano"),
           "metrics": ("otel.shop_otel_metrics", "time_unix_nano")}
# what each table is broken down by in the check
BY = {"spans": "service_name", "logs": "severity_text", "metrics": "name"}
MARGIN_S = 5            # around a run's start and end, for the records stamped at its edges


def expected_columns() -> dict[str, list[str]]:
    """Top-level columns of each table, read from the DDL: two spaces of indentation, a name and an upper-case type."""
    return {statement.split("(")[0].split()[-1]: re.findall(r"^  ([a-z_]+) [A-Z]", statement, flags=re.M)
            for statement in statements(DDL)}


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


def lines(path: Path) -> int:
    with path.open(encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def last_run() -> tuple[Path, float, float] | None:
    """The newest run of the sim that sent over OTLP, with its start and end as epoch seconds."""
    runs = sorted(path for path in RUNS.glob("run-*") if (path / "otlp.json").exists()) if RUNS.exists() else []
    if not runs:
        return None
    run = runs[-1]
    start = datetime.strptime(run.name[4:19], "%Y%m%d-%H%M%S").timestamp()
    end_file = run / "events.jsonl"
    end = end_file.stat().st_mtime if end_file.exists() else max(path.stat().st_mtime for path in run.iterdir())
    return run, start, end


# ---- check ------------------------------------------------------------------------------------------------

def check_tables(w, env: dict[str, str]) -> list[str]:
    from databricks.sdk.errors import NotFound
    section("OTel tables")
    present, expected = [], expected_columns()
    for signal, (table, _) in SIGNALS.items():
        try:
            table_info = w.tables.get(f"{env['UC_CATALOG']}.{table}")
        except NotFound:
            fail(f"{table} does not exist: python setup/02_otel.py create")
            continue
        except Exception as error:  # noqa: BLE001
            fail(f"{table}: {scrub(first_line(error))}")
            continue
        names = [column.name for column in sorted(table_info.columns or [], key=lambda column: column.position or 0)]
        if names != expected[table]:
            fail(f"{table} does not have the columns of sql/phase2/otel.sql: drop it and run create")
            continue
        version = (table_info.properties or {}).get("otel.schemaVersion")
        if version is None:
            warn(f"{table}: {len(names)} columns, but otel.schemaVersion could not be read")
        elif version != SCHEMA_VERSION:
            fail(f"{table}: otel.schemaVersion is {version}, the docs ask for {SCHEMA_VERSION}")
            continue
        else:
            ok(f"{table}: {len(names)} columns, otel.schemaVersion = {version}")
        present.append(signal)
    return present


def check_grants(w, env: dict[str, str], present: list[str]) -> None:
    section("Zerobus grants for the service principal")
    for signal in present:
        table = SIGNALS[signal][0]
        try:
            rows = sql(w, env["WAREHOUSE_ID"], f"SHOW GRANTS `{env['DATABRICKS_CLIENT_ID']}` ON TABLE {table}",
                       catalog=env["UC_CATALOG"])
        except SqlFailed as error:
            fail(f"{table}: could not read the grants ({error})")
            continue
        granted = {str(cell).upper().replace("_", " ") for row in rows for cell in row if cell}
        lacking = [privilege for privilege in ("SELECT", "MODIFY") if privilege not in granted]
        if lacking:
            fail(f"{table} lacks {' and '.join(lacking)}: python setup/02_otel.py create")
        else:
            ok(f"SELECT and MODIFY on {table}")


def check_rows(w, env: dict[str, str], present: list[str]) -> None:
    section("Rows per signal")
    for signal in present:
        table = SIGNALS[signal][0]
        try:
            count, age = sql(w, env["WAREHOUSE_ID"], "SELECT count(*), timestampdiff(SECOND, max(time), "
                             f"current_timestamp()) FROM {table}", catalog=env["UC_CATALOG"])[0]
            parts = sql(w, env["WAREHOUSE_ID"], f"SELECT {BY[signal]}, count(*) FROM {table} GROUP BY 1 ORDER BY 1",
                        catalog=env["UC_CATALOG"]) if int(count) else []
        except SqlFailed as error:
            fail(f"{table}: {error}")
            continue
        if not int(count):
            info(f"{table}: no rows yet")
            continue
        detail = ", ".join(f"{name or '(none)'} {int(rows):,}" for name, rows in parts)
        newest = f", the newest {int(age):,} s old" if age is not None else ""
        info(f"{table}: {int(count):,} rows{newest}; by {BY[signal]}: {detail}")


def check_last_run(w, env: dict[str, str], present: list[str]) -> None:
    section("Last run of the sim with --otlp")
    if (found := last_run()) is None:
        info("none yet: python sim/run.py --otlp, with the Collector running (bash ingest/otel/run.sh)")
        return
    run, start, end = found
    low, high = int((start - MARGIN_S) * 1e9), int((end + MARGIN_S) * 1e9)
    info(f"{run.relative_to(ROOT)}: {datetime.fromtimestamp(start):%H:%M:%S} to {datetime.fromtimestamp(end):%H:%M:%S}")
    for signal in ("spans", "logs"):
        if signal not in present:
            continue
        table, column = SIGNALS[signal]
        sent = lines(run / f"{signal}.jsonl")
        # a log has no ID of its own: its time and trace context tell two apart (concat_ws skips the empty ones)
        key = "span_id" if signal == "spans" else "concat_ws(':', time_unix_nano, trace_id, span_id)"
        try:
            got, distinct = sql(w, env["WAREHOUSE_ID"], f"SELECT count(*), count(DISTINCT {key}) FROM {table} "
                                f"WHERE {column} BETWEEN {low} AND {high}", catalog=env["UC_CATALOG"])[0]
        except SqlFailed as error:
            fail(f"{table}: {error}")
            continue
        got, distinct = int(got), int(distinct)
        if got == sent:
            ok(f"{signal}: all {sent:,} of the run are in {table}")
        elif got < sent:
            warn(f"{signal}: {got:,} of {sent:,} in {table}; Zerobus takes about 5 s and the Collector batches for up "
                 "to 5 s, so check again in a minute; if it stays short, look at the Collector's terminal")
        elif distinct == sent:
            warn(f"{signal}: {got:,} rows for {sent:,} of the run, {got - sent:,} repeated (delivery is at least once)")
        else:
            warn(f"{signal}: {got:,} rows in the window of the run for {sent:,} sent: another run may overlap it")
    if "metrics" in present:
        table, column = SIGNALS["metrics"]
        try:
            points = sql(w, env["WAREHOUSE_ID"], f"SELECT count(*) FROM {table} "
                         f"WHERE {column} BETWEEN {low} AND {high}", catalog=env["UC_CATALOG"])[0][0]
        except SqlFailed as error:
            fail(f"{table}: {error}")
            return
        (ok if int(points) else warn)(f"metrics: {int(points):,} data points of the run in {table}")


def check_collector() -> None:
    section("OTel Collector on the Mac")
    try:
        socket.create_connection(COLLECTOR, timeout=1).close()
        ok(f"something listens on {COLLECTOR[0]}:{COLLECTOR[1]}: the Collector of ingest/otel/run.sh")
    except OSError:
        info(f"nothing on {COLLECTOR[0]}:{COLLECTOR[1]}: bash ingest/otel/run.sh starts the Collector")


def check(env: dict[str, str]) -> int:
    if (w := _databricks(env)) is None:
        return summary()
    present = check_tables(w, env)
    check_grants(w, env, present)
    check_rows(w, env, present)
    check_last_run(w, env, present)
    check_collector()
    return summary()


# ---- create -----------------------------------------------------------------------------------------------

def create(env: dict[str, str]) -> int:
    if (w := _databricks(env)) is None:
        return summary()
    warehouse, catalog, client_id = env["WAREHOUSE_ID"], env["UC_CATALOG"], env["DATABRICKS_CLIENT_ID"]
    section("OTel tables")
    for statement in statements(DDL):
        name = statement.split("(")[0].split()[-1]
        try:
            sql(w, warehouse, statement, catalog=catalog)
            ok(f"{name} ({DDL.relative_to(ROOT)})")
        except SqlFailed as error:
            fail(f"{name}: {error}")
    section("Zerobus grants for the service principal")
    for table, _ in SIGNALS.values():
        try:
            sql(w, warehouse, f"GRANT SELECT, MODIFY ON TABLE {table} TO `{client_id}`", catalog=catalog)
            ok(f"SELECT and MODIFY on {table}")
        except SqlFailed as error:
            fail(f"{table}: {error}")
    info("USE CATALOG and USE SCHEMA on otel came in Phase 0: python setup/00_environment.py check shows them")
    return summary()


def main() -> int:
    parser = argparse.ArgumentParser(description="zerobus-nrt-oncall · Phase 2 OpenTelemetry")
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("check", help="read-only check of the OTel tables, grants, rows and the Collector port")
    actions.add_parser("create", help="creates the three OTel tables and the Zerobus grants")
    args = parser.parse_args()
    env = read_env()
    return check(env) if args.action == "check" else create(env)


if __name__ == "__main__":
    sys.exit(guard(main))
