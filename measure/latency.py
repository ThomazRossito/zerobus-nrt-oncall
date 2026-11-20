#!/usr/bin/env python3
# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
"""Phase 2 · commit → bronze latency of the CDC: from a commit in the local Postgres to the row being queryable in
bronze, through Debezium Server and Zerobus Ingest.

Ported from part 1 (zerobus-postgres-cdc, measure/latency.py), with the same ruler: the clock of this machine right
after the commit, bronze polled on the SQL warehouse --poll seconds after the previous query returned, and the time
the query that finds the row returns. A writer thread inserts probe rows into cmdb.alerts_history at a fixed rate,
one commit each, with rule = 'latency_probe' and alert_id above every alert_id in Postgres and in bronze. The main
thread asks bronze for the probes it has not seen yet, at most 500 per query, the oldest first.

Usage, from the project root with the conda env active (conda activate zerobus-nrt-oncall), with the CDC running
in another terminal (DBZ_SNAPSHOT_MODE=no_data bash ingest/debezium/run.sh):
  python measure/latency.py                     10 min, 5 inserts/s, bronze polled every 2 s (part 1's interval)
  python measure/latency.py --minutes 2 --rate 1 --poll 1 --grace 60

Prints p50, p95 and max of the latency at the return of the query that found each row (part 1's number, an upper
bound) and at the start of the last query that missed it (a lower bound), and the split the row itself carries:
__source_ts_ms (the change in Postgres), then __ts_ms (Debezium processed it), then the query. Saves every probe in
.local/phase2_latency-<time>.csv and the summary in .local/phase2_latency-<time>.json.
Reads from .env: DATABRICKS_CONFIG_PROFILE, UC_CATALOG, WAREHOUSE_ID and the PG_* admin variables. Never reads
DATABRICKS_CLIENT_SECRET: it removes it and DATABRICKS_CLIENT_ID from this process, unread, so the queries run with
the CLI profile even in a terminal that sourced .env. The output shows no host, e-mail or IDs: safe to paste in a chat.
"""
from __future__ import annotations

import argparse
import csv
import os
import queue
import statistics
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from itertools import pairwise
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.common import (ROOT, SqlFailed, client, fail, first_line, guard, info, missing, ok,  # noqa: E402
                        pg_connect, pg_error, read_env, save_local, scrub, section, sql, summary, warn)
from sim.scenario import SLOT  # noqa: E402

NEEDED = ("DATABRICKS_CONFIG_PROFILE", "UC_CATALOG", "WAREHOUSE_ID", "PG_HOST", "PG_PORT", "PG_DBNAME",
          "PG_USER_ADMIN", "PG_PASSWORD_ADMIN")
# exported for Debezium by "set -a; source .env": the databricks-sdk reads them too and would mix them with the profile
SERVICE_PRINCIPAL = ("DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET")
RULE = "latency_probe"
PROBE_SERVICES = ("checkout", "payment", "inventory")
INSERT = ("INSERT INTO cmdb.alerts_history (alert_id, service, rule, severity, fired_at, value) "
          "VALUES (%s, %s, %s, 'sev3', now(), %s)")
PER_QUERY = 500            # probes per query, as in part 1: the IN list stays small
PROGRESS_S = 15            # a progress line every 15 s
FAILED_IN_A_ROW = 5        # queries that fail one after the other before the run stops


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def clock() -> str:
    return time.strftime("%H:%M:%S")


def as_int(value: object) -> int | None:
    return None if value is None else int(value)


def p95(values: list[int]) -> int:
    """Part 1's p95: a position in the sorted list; with fewer than 20 values, the max."""
    return sorted(values)[int(len(values) * 0.95) - 1] if len(values) >= 20 else max(values)


def duration(ms: float) -> str:
    return f"{ms:.0f} ms" if abs(ms) < 1000 else f"{ms / 1000:.1f} s"


def spread(values: list[int]) -> str:
    return f"p50 {duration(statistics.median(values))} · p95 {duration(p95(values))} · max {duration(max(values))}"


def numbers(values: list[int]) -> dict[str, float] | None:
    return {"p50": statistics.median(values), "p95": p95(values), "max": max(values)} if values else None


@dataclass
class Probe:
    alert_id: int
    commit_ms: int                    # this machine's clock, right after the commit returned
    missed_ms: int | None = None      # start of the last query that asked for it and did not find it
    seen_ms: int | None = None        # return of the query that found it
    source_ms: int | None = None      # __source_ts_ms: when the change was made in Postgres
    debezium_ms: int | None = None    # __ts_ms: when Debezium processed the change


@dataclass
class Run:
    first_id: int
    bronze_rows: int
    probes: dict[int, Probe] = field(default_factory=dict)
    pending: dict[int, Probe] = field(default_factory=dict)     # in commit order: the oldest first
    query_ms: list[int] = field(default_factory=list)           # how long each query took
    starts: list[int] = field(default_factory=list)             # when each query started
    failed: int = 0
    stopped: str = ""          # why the run ended early, if it did
    fatal: bool = False        # the reason is a failure, not a Ctrl+C
    cut_short: bool = False    # it ended before the grace was over
    # wall clock minus monotonic clock at the start: if it moves, the Mac's clock was set during the run
    offset_s: float = field(default_factory=lambda: time.time() - time.monotonic())


class Writer(threading.Thread):
    """Inserts one probe every 1/rate seconds on its own connection; autocommit makes each insert one commit."""

    def __init__(self, env: dict[str, str], first_id: int, rate: float, seconds: float):
        super().__init__(name="probe-writer", daemon=True)
        self.env, self.first_id, self.rate, self.seconds = env, first_id, rate, seconds
        self.sent: queue.SimpleQueue[tuple[int, int]] = queue.SimpleQueue()
        self.count = 0
        self.error: str | None = None
        self.halt = threading.Event()

    def run(self) -> None:
        try:
            with pg_connect(self.env) as conn:
                start = time.monotonic()
                while self.count / self.rate < self.seconds:
                    # the schedule is fixed from the start, so a slow insert does not slow the rate down
                    if self.halt.wait(max(0.0, start + self.count / self.rate - time.monotonic())):
                        return
                    alert_id = self.first_id + self.count
                    conn.execute(INSERT, (alert_id, PROBE_SERVICES[self.count % 3], RULE, float(self.count)))
                    self.sent.put((alert_id, now_ms()))
                    self.count += 1
        except Exception as error:  # noqa: BLE001 - the main thread reports it
            self.error = pg_error(error, self.env)


def preflight(env: dict[str, str]) -> tuple[object, int, int] | None:
    """The client, the first probe alert_id and the bronze row count, or None after a FAIL line."""
    section("Before the probes")
    if keys := missing(env, NEEDED):
        fail("missing in .env: " + ", ".join(keys))
        return None
    for name in SERVICE_PRINCIPAL:
        os.environ.pop(name, None)
    profile = env["DATABRICKS_CONFIG_PROFILE"]
    try:
        w = client(env)
        w.current_user.me()
    except Exception as error:  # noqa: BLE001
        fail(f"profile {profile}: {scrub(first_line(error))}")
        return None
    ok(f"profile {profile}: authenticated")
    # this query also wakes the warehouse up, so the first probes do not wait for it to start
    try:
        rows, inserts, top = sql(w, env["WAREHOUSE_ID"], "SELECT count(*), count_if(__op = 'c'), "
                                 "coalesce(max(alert_id), 0) FROM bronze.alerts_history", catalog=env["UC_CATALOG"])[0]
    except SqlFailed as error:
        fail(f"bronze.alerts_history: {error}")
        return None
    ok(f"bronze.alerts_history: {int(rows):,} rows ({int(inserts):,} inserts from earlier runs); the warehouse answers")
    try:
        with pg_connect(env, dbname="postgres") as conn:
            slot = conn.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (SLOT,)).fetchone()
        with pg_connect(env) as conn:
            high = conn.execute("SELECT coalesce(max(alert_id), 0) FROM cmdb.alerts_history").fetchone()[0]
    except Exception as error:  # noqa: BLE001
        fail(f"local Postgres: {pg_error(error, env)}")
        return None
    if slot is None or not slot[0]:
        fail(f"slot {SLOT} {'does not exist' if slot is None else 'is inactive'}: start the CDC first, "
             "DBZ_SNAPSHOT_MODE=no_data bash ingest/debezium/run.sh")
        return None
    ok(f"slot {SLOT} active: Debezium is streaming")
    # above Postgres and bronze: a probe never takes the alert_id of a row bronze already holds
    first = max(int(high), int(top)) + 1
    ok(f"probes from alert_id {first:,} up, with rule = '{RULE}'")
    return w, first, int(rows)


def drain(writer: Writer, run: Run) -> None:
    while True:
        try:
            alert_id, commit_ms = writer.sent.get_nowait()
        except queue.Empty:
            return
        run.probes[alert_id] = run.pending[alert_id] = Probe(alert_id, commit_ms)


def progress(run: Run) -> None:
    seen = [probe.seen_ms - probe.commit_ms for probe in run.probes.values() if probe.seen_ms is not None]
    line = f"{clock()}  sent {len(run.probes):,} · seen {len(seen):,} · waiting {len(run.pending):,}"
    info(line + (f" · {spread(seen)}" if seen else ""))


def poll(w, env: dict[str, str], run: Run) -> str | None:
    """One query for the oldest probes not seen yet; returns the error of a failed query."""
    batch = list(run.pending.values())[:PER_QUERY]
    ids = ",".join(str(probe.alert_id) for probe in batch)
    start = now_ms()
    try:
        rows = sql(w, env["WAREHOUSE_ID"], "SELECT alert_id, min(__source_ts_ms), min(__ts_ms) "
                                           f"FROM bronze.alerts_history WHERE alert_id IN ({ids}) AND __op = 'c' "
                                           "GROUP BY alert_id", wait_s=60, catalog=env["UC_CATALOG"])
    except SqlFailed as error:
        run.failed += 1
        return str(error)
    end = now_ms()
    run.query_ms.append(end - start)
    run.starts.append(start)
    found = {int(row[0]): row for row in rows}
    for probe in batch:
        if (row := found.get(probe.alert_id)) is None:
            probe.missed_ms = start
            continue
        probe.seen_ms, probe.source_ms, probe.debezium_ms = end, as_int(row[1]), as_int(row[2])
        del run.pending[probe.alert_id]
    return None


def measure(w, env: dict[str, str], run: Run, args: argparse.Namespace) -> int:
    writer = Writer(env, run.first_id, args.rate, args.minutes * 60)
    in_a_row = 0
    section(f"Probes: {args.rate:g} per second for {args.minutes:g} min, bronze polled {args.poll:g} s after each "
            "query returns")
    info("Ctrl+C stops the inserts and waits for the probes on the way; a second Ctrl+C stops at once")
    writer.start()
    grace_until = None
    next_progress = time.monotonic() + PROGRESS_S
    while True:
        try:
            drain(writer, run)
            if writer.error and not run.fatal:
                # the probes already committed still count: the grace below waits for them
                run.stopped, run.fatal = f"the inserts stopped: {writer.error}", True
            if not writer.is_alive():
                drain(writer, run)
                grace_until = grace_until or time.monotonic() + args.grace
                if not run.pending or time.monotonic() >= grace_until:
                    break
            if run.pending:
                if (error := poll(w, env, run)) is None:
                    in_a_row = 0
                else:
                    in_a_row += 1
                    warn(f"{clock()}  a query failed, trying again at the next poll: {error}")
                    if in_a_row >= FAILED_IN_A_ROW:
                        run.stopped, run.fatal, run.cut_short = f"{in_a_row} queries failed in a row", True, True
                        break
            if time.monotonic() >= next_progress:
                progress(run)
                next_progress += PROGRESS_S
            time.sleep(args.poll)
        except KeyboardInterrupt:
            if writer.halt.is_set() or not writer.is_alive():
                run.stopped = run.stopped or "stopped with Ctrl+C before the last probes arrived"
                run.cut_short = True
                break
            # the first Ctrl+C ends the inserts; the probes on the way are still measured during the grace
            writer.halt.set()
            run.stopped = "inserts stopped with Ctrl+C"
            info(f"{clock()}  inserts stopped; waiting up to {args.grace} s for the probes on the way, "
                 "Ctrl+C again to stop now")
    writer.halt.set()
    writer.join(timeout=10)
    drain(writer, run)
    return report(args, run)


def report(args: argparse.Namespace, run: Run) -> int:
    section("Result")
    probes, query_ms, failed = run.probes, run.query_ms, run.failed
    if run.stopped:
        (fail if run.fatal else warn)(run.stopped)
    seen = [probe for probe in probes.values() if probe.seen_ms is not None]
    if not probes:
        fail("no probe was inserted")
        return summary()
    if not seen:
        if run.query_ms:
            fail(f"{len(probes):,} probe(s) inserted and none showed up in bronze: is Debezium streaming? "
                 "python setup/02_cdc.py check")
        elif not run.fatal:
            warn("no probe was measured")
        return summary()
    absent = len(probes) - len(seen)
    late = (f", {absent:,} still on the way when it stopped" if run.cut_short else
            f", {absent:,} not seen in the {args.grace} s after the last insert")
    (warn if absent else ok)(f"{len(probes):,} probes inserted, {len(seen):,} seen in bronze"
                             + (late if absent else ""))
    upper = [probe.seen_ms - probe.commit_ms for probe in seen]
    lower = [max(0, (probe.missed_ms or probe.commit_ms) - probe.commit_ms) for probe in seen]
    split = [probe for probe in seen if probe.source_ms is not None and probe.debezium_ms is not None]
    capture = [probe.debezium_ms - probe.source_ms for probe in split]
    delivery = [probe.seen_ms - probe.debezium_ms for probe in split]
    skew = statistics.median(probe.commit_ms - probe.source_ms for probe in split) if split else None
    lines = [("at the return of the query that found it (part 1's number)", upper),
             ("at the start of the last query that missed it (lower bound)", lower)]
    if split:
        lines += [("capture: __source_ts_ms → __ts_ms (Debezium processed it)", capture),
                  ("the rest: __ts_ms → the return of the query", delivery)]
    info("commit in Postgres → row queryable in bronze")
    for label, values in lines:
        info(f"  {label:<60} {spread(values)}")
    gaps = [later - earlier for earlier, later in pairwise(run.starts)]
    if query_ms:
        info(f"{len(query_ms):,} queries, {spread(query_ms)} each"
             + (f", one every {duration(statistics.mean(gaps))} on average" if gaps else "")
             + (f", {failed} failed" if failed else ""))
    if skew is not None:
        # both clocks are this machine's when Postgres runs on it: the commit returns a few ms after Postgres stamps it
        (info if abs(skew) < 1000 else warn)(
            f"clock check: commit seen here minus __source_ts_ms, median {duration(skew)}"
            + ("" if abs(skew) < 1000 else ": the Postgres clock differs from this one, so the capture line is off"))
    step = (time.time() - time.monotonic() - run.offset_s) * 1000
    if abs(step) >= 100:
        warn(f"the clock of this machine moved {duration(step)} during the run: the probes around that moment are off")
    info("the poll interval sets the resolution: the real latency lies between the first two lines")
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    folder = ROOT / ".local"
    folder.mkdir(exist_ok=True)
    table = folder / f"phase2_latency-{stamp}.csv"
    with table.open("w", newline="", encoding="utf-8") as handle:
        out = csv.writer(handle)
        out.writerow(["alert_id", "commit_ms", "missed_ms", "seen_ms", "source_ts_ms", "debezium_ts_ms", "latency_ms"])
        for probe in probes.values():
            latency = probe.seen_ms - probe.commit_ms if probe.seen_ms is not None else None
            out.writerow([probe.alert_id, probe.commit_ms, probe.missed_ms, probe.seen_ms, probe.source_ms,
                          probe.debezium_ms, latency])
    saved = save_local(f"phase2_latency-{stamp}.json", {
        "run": stamp, "rate_per_s": args.rate, "minutes": args.minutes, "poll_s": args.poll, "grace_s": args.grace,
        "first_alert_id": run.first_id, "bronze_rows_before": run.bronze_rows, "probes": len(probes),
        "seen": len(seen), "stopped": run.stopped or None, "queries": len(query_ms), "failed_queries": failed,
        "mean_query_interval_ms": round(statistics.mean(gaps)) if gaps else None,
        "latency_ms": {"upper": numbers(upper), "lower": numbers(lower), "capture": numbers(capture),
                       "after_debezium": numbers(delivery), "query": numbers(query_ms)},
        "clock_skew_ms": skew, "clock_step_ms": round(step),
    })
    ok(f"saved {table.relative_to(ROOT)} and {saved.name}")
    return summary()


def main() -> int:
    parser = argparse.ArgumentParser(description="zerobus-nrt-oncall · Phase 2 commit → bronze latency")
    parser.add_argument("--minutes", type=float, default=10.0, help="minutes of inserts (default 10)")
    parser.add_argument("--rate", type=float, default=5.0, help="probe inserts per second (default 5)")
    parser.add_argument("--poll", type=float, default=2.0,
                        help="seconds between a query's return and the next query (default 2, as in part 1)")
    parser.add_argument("--grace", type=int, default=60,
                        help="seconds to keep polling for the last probes after the inserts end (default 60)")
    args = parser.parse_args()
    if not (0 < args.minutes <= 120 and 0.1 <= args.rate <= 50 and 0.5 <= args.poll <= 30 and 0 <= args.grace <= 600):
        parser.error("--minutes above 0 up to 120, --rate from 0.1 to 50, --poll from 0.5 to 30, --grace up to 600")
    if args.minutes * 60 * args.rate < 1:
        parser.error("--minutes and --rate give less than one probe")
    env = read_env()
    if (ready := preflight(env)) is None:
        return summary()
    w, first_id, bronze_rows = ready
    return measure(w, env, Run(first_id, bronze_rows), args)


if __name__ == "__main__":
    sys.exit(guard(main))
