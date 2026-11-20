#!/usr/bin/env python3
# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
"""Phase 2 · the initial load in parallel: N Debezium Server instances, each one reading a range of
cmdb.alerts_history.alert_id and writing to the same bronze table through its own Zerobus stream.

Ported from part 1 (zerobus-postgres-cdc, debezium-server/run-parallel.sh). Each instance runs with
snapshot.mode=initial_only: it takes its snapshot and does not stream. The first instance also takes cmdb.services
and cmdb.incidents. The script follows the r= counter of each instance's sink metrics line, which the sink counts
only after the flush, so the rows are already durable, and stops an instance once it delivered its whole range.
Afterwards the CDC goes on with run.sh in no_data mode, from a new replication slot.

Usage, from the project root with the conda env active (conda activate zerobus-nrt-oncall):
  set -a; source .env; set +a                   once per terminal: the JVMs read the .env variables from this shell
  python setup/02_cdc.py reset --yes            no slot, no offsets and empty bronze tables before the load
  python ingest/debezium/run_parallel.py        8 instances, batches of 51200: part 1's 94 million row load
  python ingest/debezium/run_parallel.py --instances 4 --batch 51200 --idle 180

Nobody may write to cmdb from the start of the load until the CDC is up: the ranges and counts are taken at the
start, and the new slot only sees what is written after it exists.
JAVA_OPTS (default -Xms512m -Xmx1536m) is the heap of each JVM; part 1 measured 8 instances at 99% CPU on a Mac with
10 cores and 32 GB. The terminal shows only this script's lines, safe to paste in a chat; each instance logs to
ingest/debezium/data/parallel/<run>/i<k>/debezium.log. Never reads DATABRICKS_CLIENT_SECRET: it checks that the
variable is exported, and the JVMs inherit it from this shell.
"""
from __future__ import annotations

import argparse
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ingest.mask import mask  # noqa: E402
from lib.common import (HOME, ROOT, SqlFailed, client, fail, first_line, guard, info, missing, ok,  # noqa: E402
                        pg_connect, pg_error, read_env, run, save_local, scrub, section, sql, summary, warn)
from sim.scenario import SLOT  # noqa: E402

DEBEZIUM = ROOT / "ingest" / "debezium"
DIST = DEBEZIUM / "dist" / "debezium-server"
BASE = DEBEZIUM / "application.properties"
OFFSETS = DEBEZIUM / "data" / "offsets.dat"
RUNS = DEBEZIUM / "data" / "parallel"
TABLE, KEY = "cmdb.alerts_history", "alert_id"
SMALL = ("cmdb.services", "cmdb.incidents")      # the first instance takes these two whole
SLOT_PREFIX = "oncall_par_"
HTTP_BASE = 8090                                 # Quarkus HTTP port: i0 uses 8090, i1 8091...
JAVA_OPTS = "-Xms512m -Xmx1536m"

# what the Debezium JVMs read from this shell (application.properties); the secret is only checked for presence
SHELL = ("PG_HOST", "PG_PORT", "PG_DBNAME", "PG_USER", "PG_PASSWORD", "UC_CATALOG", "ZEROBUS_SERVER_ENDPOINT",
         "DATABRICKS_WORKSPACE_URL", "DATABRICKS_CLIENT_ID")
SECRET = "DATABRICKS_CLIENT_SECRET"
DATABRICKS = ("DATABRICKS_CONFIG_PROFILE", "UC_CATALOG", "WAREHOUSE_ID")
POSTGRES = ("PG_HOST", "PG_PORT", "PG_DBNAME", "PG_USER_ADMIN", "PG_PASSWORD_ADMIN")

# keys of application.properties that each instance redefines; the rest of the file is kept as it is
OVERRIDDEN = {
    "debezium.source.snapshot.mode", "debezium.source.slot.name", "debezium.source.table.include.list",
    "debezium.source.snapshot.select.statement.overrides",
    f"debezium.source.snapshot.select.statement.overrides.{TABLE}",
    "debezium.source.offset.storage.file.filename", "debezium.source.max.batch.size", "debezium.source.max.queue.size",
    "debezium.source.snapshot.fetch.size", "debezium.sink.zerobus.max.inflight.records",
    "debezium.sink.zerobus.metrics.log.interval", "quarkus.http.port",
}

# lines of the instance logs this script reads (formats checked in the v3.8.0.Alpha2 source)
METRICS = re.compile(r"Zerobus sink metrics .*?\(c=\d+ u=\d+ d=\d+ r=(\d+)\).*?maxFlushMs=(\d+)")
WAITING = re.compile(r"Zerobus has not acknowledged the (\d+) records for table '[^']*' after (\d+) ms")
ACKED = re.compile(r"Zerobus acknowledged the (\d+) records for table '[^']*' after (\d+) ms")
SNAPSHOT_DONE = "Streaming is disabled for snapshot mode"
# the instance gave up; its JVM can still take a while to exit (on 2026-11-20 one hung closing the Zerobus streams)
FAILED = re.compile(r"Debezium engine completed with a failure|Shutting down Debezium Server with exit code [1-9]")
LOUD_WAIT_MS = 30_000                            # ack waits shown in the terminal: from 30 s on, every 30 s


class Stop(Exception):
    """SIGTERM or SIGHUP reached this script: the instances must stop too."""


@dataclass
class Part:
    k: int
    low: int            # first alert_id of the range
    high: int           # one past the last alert_id
    rows: int           # alerts_history rows in the range
    small: int = 0      # services and incidents rows, instance 0 only

    @property
    def expected(self) -> int:
        return self.rows + self.small

    @property
    def tables(self) -> str:
        return ",".join((*SMALL, TABLE)) if self.k == 0 else TABLE


@dataclass
class Plan:
    java: str
    java_major: int
    runner: Path
    parts: list[Part]
    batch: int
    java_opts: list[str]


@dataclass
class Instance:
    part: Part
    folder: Path
    proc: subprocess.Popen | None = None
    r: int = 0
    max_flush_ms: int = 0
    changed: float = 0.0
    snapshot_done: bool = False
    failed: bool = False
    warnings: int = 0
    longest_wait_ms: int = 0
    loud_bucket: int = 0
    done_s: float | None = None
    status: str = "running"
    position: int = 0
    rest: bytes = b""

    @property
    def name(self) -> str:
        return f"i{self.part.k}"

    @property
    def log(self) -> Path:
        return self.folder / "debezium.log"

    def read_log(self) -> list[str]:
        """Reads what the instance wrote since the last call; returns the lines worth showing in the terminal."""
        try:
            with self.log.open("rb") as handle:
                handle.seek(self.position)
                chunk = handle.read()
        except FileNotFoundError:
            return []
        self.position += len(chunk)
        lines = (self.rest + chunk).split(b"\n")
        self.rest = lines.pop()
        notes = []
        for raw in lines:
            line = raw.decode("utf-8", "replace")
            if found := METRICS.search(line):
                self.r, self.max_flush_ms = int(found[1]), int(found[2])
            elif found := WAITING.search(line):
                records, waited = int(found[1]), int(found[2])
                self.warnings += 1
                self.longest_wait_ms = max(self.longest_wait_ms, waited)
                if waited >= LOUD_WAIT_MS and waited // LOUD_WAIT_MS > self.loud_bucket:
                    self.loud_bucket = waited // LOUD_WAIT_MS
                    notes.append(f"{self.name}  no ack yet for {records:,} records after {waited / 1000:.0f} s")
            elif found := ACKED.search(line):
                records, waited = int(found[1]), int(found[2])
                self.longest_wait_ms = max(self.longest_wait_ms, waited)
                if waited >= LOUD_WAIT_MS:
                    notes.append(f"{self.name}  acknowledged {records:,} records after {waited / 1000:.1f} s")
                self.loud_bucket = 0
            elif SNAPSHOT_DONE in line:
                self.snapshot_done = True
            elif FAILED.search(line):
                self.failed = True
        return notes

    def tail(self, count: int = 25) -> list[str]:
        """The last lines of the log, masked like the console of run.sh."""
        try:
            lines = self.log.read_text(encoding="utf-8", errors="replace").splitlines()[-count:]
        except FileNotFoundError:
            return []
        return [mask(line, HOME).rstrip() for line in lines]

    def terminate(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            try:
                self.proc.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass


# ---- before the load --------------------------------------------------------------------------------------

def java_binary() -> str | None:
    home = os.environ.get("JAVA_HOME")
    if home and Path(home, "bin", "java").exists():
        return str(Path(home, "bin", "java"))
    return shutil.which("java")


def java_major(java: str) -> int:
    _, out, err = run([java, "-version"], timeout=30)
    found = re.search(r'version "(\d+)(?:\.(\d+))?', out + err)
    if not found:
        return 0
    return int(found[2] or 0) if found[1] == "1" else int(found[1])


def ranges(conn, instances: int) -> list[Part] | None:
    """alert_id from min to max in equal spans; a gap leaves a range with fewer rows, so each one is counted."""
    low, high = conn.execute(f"SELECT min({KEY}), max({KEY}) FROM {TABLE}").fetchone()
    if low is None:
        return None
    span = high - low + 1
    bounds = [low + k * span // instances for k in range(instances)] + [high + 1]
    filters = ", ".join(f"count(*) FILTER (WHERE {KEY} >= %s AND {KEY} < %s)" for _ in range(instances))
    params = [value for k in range(instances) for value in (bounds[k], bounds[k + 1])]
    counts = conn.execute(f"SELECT {filters} FROM {TABLE}", params).fetchone()
    return [Part(k, bounds[k], bounds[k + 1], int(counts[k])) for k in range(instances)]


def preflight(env: dict[str, str], args: argparse.Namespace) -> Plan | None:
    section("Before the load")
    absent = [name for name in SHELL if not os.environ.get(name)]
    if SECRET not in os.environ:                 # presence only: the value is never read
        absent.append(SECRET)
    if absent:
        fail("missing in this shell: " + ", ".join(absent))
        info("from the project root, export the .env first: set -a; source .env; set +a")
        return None
    ok("the .env variables are exported in this shell")

    runners = sorted(DIST.glob("debezium-server-*runner.jar"))
    if not runners or not (DIST / "lib").is_dir():
        fail("Debezium Server not found in ingest/debezium/dist: build it from the tag (docs, Phase 2)")
        return None
    java = java_binary()
    major = java_major(java) if java else 0
    if major < 21:
        fail(f"Java 21 or newer not found (found: {major or 'none'}); set JAVA_HOME or put java 21 in the PATH")
        return None
    ok(f"{runners[0].name}, Java {major}")

    # another Debezium Server would write to the same tables and the counts would not add up
    code, out, _ = run(["pgrep", "-f", r"io[.]debezium[.]server[.]Main"])
    if code == 0 and out.split():
        fail(f"a Debezium Server is already running (pid {' '.join(out.split())}): stop it first")
        return None
    if OFFSETS.exists():
        fail("ingest/debezium/data/offsets.dat exists: python setup/02_cdc.py reset --yes")
        return None
    ok("no Debezium Server running and no offsets from an earlier load")

    if keys := missing(env, POSTGRES):
        fail("missing in .env: " + ", ".join(keys))
        return None
    try:
        with pg_connect(env) as conn:
            slots = [row[0] for row in conn.execute(
                "SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s OR starts_with(slot_name, %s)",
                (SLOT, SLOT_PREFIX)).fetchall()]
            if SLOT in slots:
                fail(f"slot {SLOT} exists: python setup/02_cdc.py reset --yes (the CDC makes a new one later)")
                return None
            for name in slots:                   # left by an earlier parallel run; no Debezium is running
                conn.execute("SELECT pg_drop_replication_slot(%s)", (name,))
                ok(f"slot {name}, left by an earlier parallel run, dropped")
            parts = ranges(conn, args.instances)
            if parts is None:
                fail(f"{TABLE} is empty: python setup/01_source.py alerts")
                return None
            parts[0].small = sum(conn.execute(f"SELECT count(*) FROM {name}").fetchone()[0] for name in SMALL)
            parts = [part for part in parts if part.expected]     # a gap in alert_id can leave a range empty
    except Exception as error:  # noqa: BLE001
        fail(f"local Postgres: {pg_error(error, env)}")
        return None
    ok(f"no slot {SLOT}; {sum(part.rows for part in parts):,} rows in {TABLE}")

    if keys := missing(env, DATABRICKS):
        fail("missing in .env: " + ", ".join(keys))
        return None
    try:
        counts = sql(client(env), env["WAREHOUSE_ID"],
                     "SELECT (SELECT count(*) FROM bronze.services), (SELECT count(*) FROM bronze.incidents), "
                     "(SELECT count(*) FROM bronze.alerts_history)", catalog=env["UC_CATALOG"])[0]
    except SqlFailed as error:
        fail(f"bronze: {error}")
        return None
    except Exception as error:  # noqa: BLE001
        fail(f"Databricks: {scrub(first_line(error))}")
        return None
    if any(int(count) for count in counts):
        fail("the bronze tables are not empty (" + ", ".join(f"{int(count):,}" for count in counts)
             + " rows): python setup/02_cdc.py reset --yes")
        return None
    ok("bronze tables empty")
    opts = shlex.split(os.environ.get("JAVA_OPTS") or JAVA_OPTS)
    return Plan(java, major, runners[0], parts, args.batch, opts)


# ---- the load ---------------------------------------------------------------------------------------------

def property_key(line: str) -> str | None:
    text = line.strip()
    if not text or text[0] in "#!" or "=" not in text:
        return None
    return text.split("=", 1)[0].strip()


def instance_config(base: list[str], part: Part, instances: int, batch: int, offsets: Path) -> str:
    """application.properties without the redefined keys, then the values of this instance."""
    kept = [line for line in base if property_key(line) not in OVERRIDDEN]
    block = [
        "",
        f"# ---- parallel snapshot: instance {part.k} of {instances} (written by ingest/debezium/run_parallel.py) ----",
        "# snapshot only, no streaming; the slot name is unique per instance in case one gets created",
        "debezium.source.snapshot.mode=initial_only",
        f"debezium.source.slot.name={SLOT_PREFIX}{part.k}",
        f"debezium.source.table.include.list={part.tables}",
        f"# the range: during the snapshot, SELECT * FROM {TABLE} becomes this SELECT",
        f"debezium.source.snapshot.select.statement.overrides={TABLE}",
        f"debezium.source.snapshot.select.statement.overrides.{TABLE}="
        f"SELECT * FROM {TABLE} WHERE {KEY} >= {part.low} AND {KEY} < {part.high}",
        f"debezium.source.offset.storage.file.filename={offsets}",
        f"debezium.source.max.batch.size={batch}",
        f"debezium.source.max.queue.size={2 * batch}",
        "debezium.source.snapshot.fetch.size=20480",
        "# records in flight without an ack: twice the batch, at least the sink's default of 50000",
        f"debezium.sink.zerobus.max.inflight.records={max(2 * batch, 50_000)}",
        "# a metrics line after every batch: its r= is how run_parallel.py follows the instance",
        "debezium.sink.zerobus.metrics.log.interval=1",
        "# each JVM needs its own HTTP port",
        f"quarkus.http.port={HTTP_BASE + part.k}",
    ]
    return "\n".join(kept + block) + "\n"


def start(plan: Plan, instances: list[Instance]) -> None:
    classpath = os.pathsep.join([str(plan.runner), str(DIST / "config" / "lib"), str(DIST / "lib" / "*")])
    command = [plan.java, *plan.java_opts, "-cp", classpath, "io.debezium.server.Main"]
    for instance in instances:
        with instance.log.open("wb") as log:
            # its own session: Ctrl+C reaches only this script, which then stops every JVM in order
            instance.proc = subprocess.Popen(command, cwd=instance.folder, stdin=subprocess.DEVNULL, stdout=log,
                                             stderr=subprocess.STDOUT, start_new_session=True)
        info(f"{instance.name}  pid {instance.proc.pid}, log {instance.log.relative_to(ROOT)}")


def stop_all(instances: list[Instance], wait_s: int = 120) -> None:
    for instance in instances:
        instance.terminate()
    deadline = time.monotonic() + wait_s
    for instance in instances:
        if instance.proc is None:
            continue
        try:
            instance.proc.wait(timeout=max(1.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            instance.proc.kill()
            instance.proc.wait()
            warn(f"{instance.name} did not stop in {wait_s} s after SIGTERM: killed")


def follow(instances: list[Instance], total: int, idle_s: int, t0: float, every_s: int = 15) -> str:
    """Watches the instances until each one delivered its range. Returns done, failed or interrupted."""
    for instance in instances:
        instance.changed = t0
    last_print = t0
    while True:
        now = time.monotonic()
        for instance in instances:
            before = instance.r
            for note in instance.read_log():
                info(note)
            if instance.r != before:
                instance.changed = now
            if instance.status != "running":
                continue
            if instance.r >= instance.part.expected:
                instance.status, instance.done_s = "done", now - t0
                instance.terminate()
                ok(f"{instance.name} delivered its {instance.part.expected:,} rows at {instance.done_s:.0f} s")
            elif instance.failed or instance.proc.poll() is not None:
                instance.status = "failed"
                code = instance.proc.poll()
                why = f"exit {code}" if code is not None else "Debezium reported a failure"
                fail(f"{instance.name} stopped before delivering its range ({why}, "
                     f"{instance.r:,} of {instance.part.expected:,} rows). Last lines of its log:")
                for line in instance.tail():
                    info(line)
                return "failed"
            elif instance.snapshot_done and now - instance.changed >= max(idle_s, 3 * instance.max_flush_ms / 1000):
                instance.status, instance.done_s = "stopped", now - t0
                instance.terminate()
                warn(f"{instance.name} finished its snapshot with {instance.r:,} of {instance.part.expected:,} rows "
                     f"and no new ack for {now - instance.changed:.0f} s: stopped; check bronze")
        delivered = sum(instance.r for instance in instances)
        if all(instance.status != "running" for instance in instances):
            return "done"
        if now - last_print >= every_s:
            last_print = now
            elapsed = now - t0
            shares = [instance.r * 100 // max(instance.part.expected, 1) for instance in instances]
            info(f"[{elapsed:4.0f} s] {delivered:,} of {total:,} ({delivered * 100 // max(total, 1)}%), "
                 f"{delivered / max(elapsed, 1):,.0f} rows/s, instances {min(shares)}%-{max(shares)}%")
        time.sleep(2)


def leftover_slots(env: dict[str, str]) -> None:
    """initial_only should not create a slot (part 1, not checked on 3.8.0.Alpha2 before): look, and drop any."""
    try:
        with pg_connect(env) as conn:
            names = [row[0] for row in conn.execute(
                "SELECT slot_name FROM pg_replication_slots WHERE starts_with(slot_name, %s)", (SLOT_PREFIX,))]
            for name in names:
                conn.execute("SELECT pg_drop_replication_slot(%s)", (name,))
    except Exception as error:  # noqa: BLE001
        warn(f"could not look for the instances' slots: {pg_error(error, env)}")
        return
    if names:
        warn(f"the instances created slot(s) {', '.join(names)}: dropped, so they hold no WAL")
    else:
        ok("no replication slot left by the instances: initial_only created none")


def load(plan: Plan, env: dict[str, str], idle_s: int) -> int:
    total = sum(part.expected for part in plan.parts)
    section(f"Parallel snapshot: {len(plan.parts)} instances, batches of {plan.batch:,}")
    for part in plan.parts:
        extra = f" + {part.small:,} of services and incidents" if part.small else ""
        port = HTTP_BASE + part.k
        info(f"i{part.k}  {KEY} {part.low:,} to {part.high - 1:,}: {part.rows:,} rows{extra}, port {port}")
    info(f"total: {total:,} rows; nobody writes to cmdb until the CDC is up (the sim stays stopped)")

    run_dir = RUNS / datetime.now().strftime("%Y%m%d-%H%M%S")
    base = BASE.read_text(encoding="utf-8").splitlines()
    instances = []
    for part in plan.parts:
        folder = run_dir / f"i{part.k}"
        (folder / "config").mkdir(parents=True)
        config = instance_config(base, part, len(plan.parts), plan.batch, folder / "offsets.dat")
        (folder / "config" / "application.properties").write_text(config, encoding="utf-8")
        instances.append(Instance(part, folder))

    # the same fix as run.sh: no kubeconfig for the Kubernetes client the distribution builds at start
    os.environ["KUBERNETES_AUTH_TRYKUBECONFIG"] = "false"

    def stop_signal(signum, _frame):
        raise Stop(signal.Signals(signum).name)

    for signum in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, stop_signal)
    section("Instances")
    t0 = time.monotonic()
    outcome = "failed"
    try:
        start(plan, instances)
        outcome = follow(instances, total, idle_s, t0)
    except (KeyboardInterrupt, Stop):
        outcome = "interrupted"
        print()
        info("interrupted: stopping the instances")
    finally:
        section("Shutdown")
        if outcome == "done":
            info("waiting for the instances to close their streams...")
        stop_all(instances)
        ok("all instances stopped")
        for instance in instances:               # the last lines they wrote, then the final counters
            instance.read_log()
            if instance.status == "running":     # stopped by this script, unless it had failed on its own
                code = instance.proc.returncode if instance.proc else None
                ours = (0, None, -signal.SIGTERM, -signal.SIGKILL)
                instance.status = "failed" if instance.failed or code not in ours else "stopped early"
    leftover_slots(env)
    return report(plan, instances, total, t0, outcome, run_dir)


def report(plan: Plan, instances: list[Instance], total: int, t0: float, outcome: str, run_dir: Path) -> int:
    section("Result")
    delivered = sum(instance.r for instance in instances)
    ends = [instance.done_s for instance in instances if instance.done_s is not None]
    seconds = max(ends) if ends and outcome == "done" else time.monotonic() - t0
    rate = delivered / max(seconds, 1)
    if outcome == "done" and delivered >= total:
        ok(f"{delivered:,} rows in {seconds:.0f} s, {rate:,.0f} rows/s with {len(instances)} streams")
    elif outcome == "done":
        warn(f"{delivered:,} of {total:,} rows in {seconds:.0f} s: some instance stopped short; check bronze")
    else:
        fail(f"{outcome}: {delivered:,} of {total:,} rows acknowledged in {seconds:.0f} s")
    for instance in instances:
        end = f"{instance.done_s:.0f} s" if instance.done_s is not None else instance.status
        info(f"{instance.name}  {instance.r:,} rows, {end}, longest flush {instance.max_flush_ms / 1000:.1f} s, "
             f"{instance.warnings} ack warning(s), longest ack wait {instance.longest_wait_ms / 1000:.1f} s")
    info("the clock starts when the JVMs start; bronze only counts the writes")
    saved = save_local(f"phase2_parallel-{run_dir.name}.json", {
        "run": run_dir.name, "outcome": outcome, "instances": len(instances), "batch": plan.batch,
        "java": plan.java_major,
        "java_opts": " ".join(plan.java_opts), "rows_expected": total, "rows_acknowledged": delivered,
        "seconds": round(seconds, 1), "rows_per_s": round(rate),
        "per_instance": [{"name": instance.name, "low": instance.part.low, "high": instance.part.high,
                          "expected": instance.part.expected, "acknowledged": instance.r,
                          "status": instance.status, "done_s": instance.done_s,
                          "max_flush_ms": instance.max_flush_ms, "ack_warnings": instance.warnings,
                          "longest_ack_wait_ms": instance.longest_wait_ms} for instance in instances],
    })
    info(f"numbers saved in {saved.relative_to(ROOT)}; logs in {run_dir.relative_to(ROOT)}")
    section("Next")
    if outcome == "done":
        info("python setup/02_cdc.py check          the counts: one snapshot row per alert_id")
        info("DBZ_SNAPSHOT_MODE=no_data bash ingest/debezium/run.sh   the CDC from a new slot, no new snapshot")
    else:
        info("python setup/02_cdc.py reset --yes    before the next try: the load starts over")
    return summary()


def main() -> int:
    parser = argparse.ArgumentParser(description="zerobus-nrt-oncall · Phase 2 parallel initial load")
    parser.add_argument("--instances", type=int, default=8,
                        help="Debezium Server instances, 1 to 16 (default 8, as in part 1's 94 million row load)")
    parser.add_argument("--batch", type=int, default=51_200, help="max.batch.size of each instance (default 51200)")
    parser.add_argument("--idle", type=int, default=180,
                        help="seconds without a new ack, after an instance's snapshot ends, before stopping it "
                             "(default 180; also at least 3 times its longest flush)")
    args = parser.parse_args()
    if not 1 <= args.instances <= 16 or args.batch < 1 or args.idle < 30:
        parser.error("--instances from 1 to 16, --batch of at least 1 and --idle of at least 30")
    env = read_env()
    plan = preflight(env, args)
    if plan is None:
        return summary()
    return load(plan, env, args.idle)


if __name__ == "__main__":
    sys.exit(guard(main))
