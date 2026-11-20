#!/usr/bin/env python3
# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
"""Phase 1 · runs the on-call scenario: the shop's telemetry, the deploy bot and the incident bot.

Usage, from the project root with the conda env active (conda activate zerobus-nrt-oncall):
  python sim/run.py                 seed 42, speed 12: one scenario minute every 5 s, about 2 min in total
  python sim/run.py --speed 1       real time, 20 min
  python sim/run.py --seed 7        another seed: other traffic, same script
  python sim/run.py --otlp          Phase 2: the telemetry also goes over OTLP to the OTel Collector on 127.0.0.1:4317
                                    (bash ingest/otel/run.sh), which sends it to Zerobus

Writes to the local Postgres (cmdb.services and cmdb.incidents) and the telemetry to .local/phase1/run-<time>/.
At the end it prints the digest of the run's events; two runs with the same seed must print the same digest, at any
speed. Reads from .env: PG_HOST, PG_PORT, PG_DBNAME, PG_USER_ADMIN and PG_PASSWORD_ADMIN.
The output shows no host, user names or passwords, so it is safe to paste in a chat.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.common import (ROOT, fail, guard, info, missing, ok, pg_connect, pg_error, read_env,  # noqa: E402
                        section, summary, warn)
from sim.bots import DeployBot, IncidentBot  # noqa: E402
from sim.scenario import (DEFAULT_SEED, DEFAULT_SPEED, DURATION_S, SERVICES, TIMELINE, clock)  # noqa: E402
from sim.shop import Shop  # noqa: E402

POSTGRES = ("PG_HOST", "PG_PORT", "PG_DBNAME", "PG_USER_ADMIN", "PG_PASSWORD_ADMIN")
RUNS = ROOT / ".local" / "phase1"
WATCH = "checkout"                 # the service whose minutes are printed during the run


class Scenario:
    """Applies the timeline to the shop and the bots, and keeps the canonical events of the run."""

    def __init__(self, shop: Shop, deploys: DeployBot, incidents: IncidentBot, t0: float, speed: float):
        self.shop, self.deploys, self.incidents = shop, deploys, incidents
        self.t0, self.speed = t0, speed
        self.events: list[dict] = []
        self._incidents: dict[str, dict] = {}     # timeline key -> {"id", "number", "service"}

    def real(self, second: int) -> datetime:
        """The real instant of a scenario second in this run."""
        return datetime.fromtimestamp(self.t0 + second / self.speed, tz=timezone.utc)

    def apply(self, second: int, action: str, details: dict) -> str:
        at = self.real(second)
        if action == "deploy":
            service, version = details["service"], details["version"]
            self.shop.deploy(service, version, second, int(at.timestamp() * 1e9))
            if details["record"]:
                self.deploys.record(service, version, at)
            self.events.append({"t": second, "kind": "deploy", "service": service, "version": version,
                                "recorded": details["record"]})
            what = "rollback, " if details.get("rollback") else ""
            told = "recorded in the catalog right away" if details["record"] else "the catalog was not told"
            return f"deploy: {service} {version} is live ({what}{told})"
        if action == "catalog_record":
            service, version, live_at = details["service"], details["version"], details["deployed_at_s"]
            self.deploys.record(service, version, self.real(live_at))
            self.events.append({"t": second, "kind": "catalog_record", "service": service, "version": version,
                                "deployed_t": live_at})
            late = (second - live_at) // 60
            return f"catalog: {service} {version} recorded with deployed_at {clock(live_at)}, {late} min late"
        if action == "incident_open":
            service = details["service"]
            suspected = self.deploys.catalog_version(service)
            incident_id = self.incidents.open(service, details["severity"], details["title"], suspected, at)
            number = len(self._incidents) + 1
            self._incidents[details["key"]] = {"id": incident_id, "number": number, "service": service}
            self.events.append({"t": second, "kind": "incident_open", "incident": number, "service": service,
                                "severity": details["severity"], "title": details["title"],
                                "suspected_version": suspected})
            return (f"incident #{number} opened: {details['severity']} \"{details['title']}\", "
                    f"blames {service} {suspected} (from the catalog)")
        incident = self._incidents[details["key"]]
        if action == "incident_status":
            self.incidents.set_status(incident["id"], details["status"], at)
            self.events.append({"t": second, "kind": "incident_status", "incident": incident["number"],
                                "status": details["status"]})
            return f"incident #{incident['number']}: {details['status']}"
        if action == "incident_suspect":
            suspected = self.deploys.catalog_version(incident["service"])
            self.incidents.set_suspect(incident["id"], suspected, at)
            self.events.append({"t": second, "kind": "incident_suspect", "incident": incident["number"],
                                "suspected_version": suspected})
            return (f"incident #{incident['number']}: now blames {incident['service']} {suspected} "
                    "(the catalog caught up)")
        if action == "incident_delete":
            self.incidents.delete(incident["id"])
            self.events.append({"t": second, "kind": "incident_delete", "incident": incident["number"]})
            return f"incident #{incident['number']} deleted (duplicate)"
        raise ValueError(f"unknown action {action}")


def minute_line(row: dict) -> str:
    rate = 100 * row["errors"] / row["requests"] if row["requests"] else 0.0
    return (f"{row['service']} {row['version']}  {row['requests']:>3} req  {rate:4.1f}% errors  "
            f"p95 {row['p95_ms']:.0f} ms")


def compare(digest: str, seed: int, speed: float, folder: Path, events: int) -> None:
    history = RUNS / "digests.jsonl"
    lines = history.read_text(encoding="utf-8").splitlines() if history.exists() else []
    previous = [json.loads(line) for line in lines]
    same_seed = [run for run in previous if run["seed"] == seed]
    with history.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"run": folder.name, "seed": seed, "speed": speed, "events": events,
                                 "digest": digest}) + "\n")
    ok(f"{events} events, digest {digest[:12]}")
    if not same_seed:
        info(f"first run with seed {seed}: run it again to compare")
    elif same_seed[-1]["digest"] == digest:
        ok(f"same events as the previous run with seed {seed} ({same_seed[-1]['run']})")
    else:
        fail(f"events differ from the previous run with seed {seed} ({same_seed[-1]['run']}, "
             f"digest {same_seed[-1]['digest'][:12]})")


def collector_listens(endpoint: str) -> bool:
    host, _, port = endpoint.rpartition(":")
    try:
        socket.create_connection((host or "127.0.0.1", int(port)), timeout=1).close()
        return True
    except (OSError, ValueError):
        return False


def run(env: dict[str, str], speed: float, seed: int, otlp: str | None = None) -> int:
    section(f"Scenario (seed {seed}, speed {speed:g}: one scenario minute every {60 / speed:.1f} s)")
    if keys := missing(env, POSTGRES):
        fail("missing in .env: " + ", ".join(keys))
        return summary()
    try:
        conn = pg_connect(env)
    except Exception as error:  # noqa: BLE001
        fail(f"could not connect as PG_USER_ADMIN: {pg_error(error, env)}")
        return summary()
    with conn:
        deploys, incidents = DeployBot(conn), IncidentBot(conn)
        try:
            current = {service: deploys.catalog_version(service) for service in SERVICES}
        except Exception as error:  # noqa: BLE001
            fail(f"could not read cmdb.services: {pg_error(error, env)} (python setup/01_source.py create)")
            return summary()
        off = [f"{service} {version}" for service, version in current.items()
               if version != SERVICES[service]["version"]]
        if off:
            fail(f"catalog not in the initial state ({', '.join(off)}): python setup/01_source.py reset")
            return summary()
        ok("catalog in the initial state: " + ", ".join(f"{service} {version}" for service, version in current.items()))
        if otlp and not collector_listens(otlp):
            fail(f"nothing listens on {otlp}: start the OTel Collector first (bash ingest/otel/run.sh)")
            return summary()
        folder, suffix = RUNS / f"run-{datetime.now():%Y%m%d-%H%M%S}", 1
        while folder.exists():
            suffix += 1
            folder = folder.with_name(f"{folder.name.split('.')[0]}.{suffix}")
        folder.mkdir(parents=True)
        info(f"telemetry in {folder.relative_to(ROOT)}/")
        if otlp:
            # tells python setup/02_otel.py check that this run went over OTLP too
            (folder / "otlp.json").write_text(json.dumps({"endpoint": otlp}) + "\n", encoding="utf-8")
            ok(f"OTel Collector listening on {otlp}: the telemetry goes over OTLP too")

        section("Timeline")
        rng = random.Random(seed)
        shop = Shop(folder, metric_interval_s=60 / speed, otlp=otlp)
        t0 = time.time()
        scenario = Scenario(shop, deploys, incidents, t0, speed)
        schedule: dict[int, list] = {}
        for second, action, details in TIMELINE:
            schedule.setdefault(second, []).append((action, details))
        lag = 0.0
        try:
            for second in range(DURATION_S + 1):
                target = t0 + second / speed
                wait = target - time.time()
                if wait > 0:
                    time.sleep(wait)
                else:
                    lag = max(lag, -wait)
                if second and second % 60 == 0:
                    for row in shop.close_minute(second // 60 - 1):
                        scenario.events.append(row)
                        if row["service"] == WATCH:
                            print(f"  {clock(second - 60)}  {minute_line(row)}")
                for action, details in schedule.get(second, []):
                    print(f"  {clock(second)}  {scenario.apply(second, action, details)}")
                if second < DURATION_S:
                    shop.serve(second, rng, int(target * 1e9), int(1e9 / speed))
        except KeyboardInterrupt:
            shop.shutdown()
            fail("interrupted: python setup/01_source.py reset puts the catalog back in the initial state")
            return summary()
        metric_exports = shop.shutdown()
        final = {service: deploys.catalog_version(service) for service in SERVICES}

    section("Result")
    canonical = "".join(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n" for event in scenario.events)
    (folder / "events.jsonl").write_text(canonical, encoding="utf-8")
    compare(hashlib.sha256(canonical.encode("utf-8")).hexdigest(), seed, speed, folder, len(scenario.events))
    ok(f"telemetry: {shop.counts['spans']:,} spans, {shop.counts['logs']:,} logs, {metric_exports} metric exports")
    if otlp:
        info(f"also sent over OTLP to {otlp}; python setup/02_otel.py check shows what reached the otel tables")
    back = all(final[service] == SERVICES[service]["version"] for service in SERVICES)
    (ok if back else warn)("catalog at the end: "
                           + ", ".join(f"{service} {version}" for service, version in final.items()))
    if lag > 1:
        warn(f"the run fell up to {lag:.1f} s behind real time: try a lower --speed")
    return summary()


def main() -> int:
    parser = argparse.ArgumentParser(description="zerobus-nrt-oncall · Phase 1 scenario")
    parser.add_argument("--speed", type=float, default=DEFAULT_SPEED,
                        help=f"scenario seconds per real second (default {DEFAULT_SPEED:g}; 1 is real time)")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help=f"traffic seed (default {DEFAULT_SEED})")
    parser.add_argument("--otlp", nargs="?", const="127.0.0.1:4317", metavar="HOST:PORT",
                        help="also send the telemetry over OTLP gRPC to the OTel Collector (default 127.0.0.1:4317)")
    args = parser.parse_args()
    if not 0 < args.speed <= 600:
        parser.error("--speed must be between 0 and 600")
    return run(read_env(), args.speed, args.seed, args.otlp)


if __name__ == "__main__":
    sys.exit(guard(main))
