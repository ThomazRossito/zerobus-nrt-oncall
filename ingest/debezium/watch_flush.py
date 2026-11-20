#!/usr/bin/env python3
# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
"""Phase 2 · scenario 6 of debezium/dbz#3026: reads the Zerobus sink's flush gauges every 200 ms while Debezium runs.

Debezium Server keeps them in JMX (debezium.zerobus:type=connector-metrics,context=sink,server=grpc,task=0):
CurrentFlushDurationMillis, the wait in progress (0 with no wait), LastFlushDurationMillis and MaxFlushDurationMillis.
With DBZ_METRICS_PORT set, ingest/debezium/run.sh starts the distribution's Prometheus JMX exporter on 127.0.0.1 and
this script reads it over HTTP. It prints a progress line every 15 s, a summary when Debezium stops or on Ctrl+C, and
keeps every sample in .local/phase2_flush-<time>.csv.

Usage, in another terminal, with Debezium started with DBZ_METRICS_PORT=9404:
  python ingest/debezium/watch_flush.py                  port 9404, a sample every 200 ms
  python ingest/debezium/watch_flush.py --port 9404 --every-ms 200
Reads nothing from .env and talks only to 127.0.0.1, so the output is safe to paste in a chat.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from lib.common import ROOT, fail, guard, info, ok, section, summary, warn  # noqa: E402

# the exporter turns debezium.zerobus<type=connector-metrics, context=sink, server=grpc, task=0>X into a gauge whose
# name ends in X, with the labels plugin, name, task and context (config/metrics.yml of the distribution)
GAUGE = re.compile(r"^\w*?(CurrentFlushDurationMillis|LastFlushDurationMillis|MaxFlushDurationMillis|TotalFlushes)"
                   r"\{([^}]*)\}\s+([-+0-9.eE]+)\s*$")
KEYS = {"CurrentFlushDurationMillis": "current", "LastFlushDurationMillis": "last",
        "MaxFlushDurationMillis": "max", "TotalFlushes": "flushes"}
PROGRESS_S = 15
GONE_S = 5                     # endpoint down this long after it answered once: Debezium stopped


def scrape(url: str) -> dict[str, float] | None:
    """The flush gauges of the Zerobus sink ({} while the sink is not up yet), or None when the exporter is down."""
    try:
        with urllib.request.urlopen(url, timeout=1) as response:  # noqa: S310 - fixed local URL
            text = response.read().decode("utf-8", "replace")
    except (urllib.error.URLError, OSError):
        return None
    found = {}
    for line in text.splitlines():
        match = GAUGE.match(line)
        if match and "zerobus" in match[2] and 'context="sink"' in match[2]:
            found[KEYS[match[1]]] = float(match[3])
    return found if {"current", "last", "max"} <= found.keys() else {}


def percentile(values: list[float], share: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * share))] if ordered else 0.0


def main() -> int:
    parser = argparse.ArgumentParser(description="zerobus-nrt-oncall · Phase 2 flush gauges (scenario 6)")
    parser.add_argument("--port", type=int, default=9404, help="port of the exporter on 127.0.0.1 (default 9404)")
    parser.add_argument("--every-ms", type=int, default=200, help="time between samples (default 200)")
    args = parser.parse_args()
    url = f"http://127.0.0.1:{args.port}/metrics"
    folder = ROOT / ".local"
    folder.mkdir(exist_ok=True)
    out = folder / f"phase2_flush-{datetime.now():%Y%m%d-%H%M%S}.csv"

    section(f"Flush gauges of the Zerobus sink, every {args.every_ms} ms, from 127.0.0.1:{args.port}")
    samples = 0
    answered = False
    last_seen = time.monotonic()
    waits: list[float] = []           # the highest CurrentFlushDurationMillis seen in each wait
    lasts: list[float] = []           # LastFlushDurationMillis right after each wait
    in_wait, peak = False, 0.0
    busy = 0                          # samples with a wait in progress; the others read 0
    max_gauge = 0.0
    flushes: list[float] = []         # TotalFlushes at the first and the last sample
    started = next_print = time.monotonic()
    with out.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["time", "current_ms", "last_ms", "max_ms"])
        try:
            while True:
                tick = time.monotonic()
                gauges = scrape(url)
                if gauges is None:
                    if answered and tick - last_seen >= GONE_S:
                        info("the exporter stopped answering: Debezium stopped")
                        break
                    if not answered and tick - started >= 2 and samples == 0 and tick >= next_print:
                        info(f"waiting for the exporter on 127.0.0.1:{args.port} (DBZ_METRICS_PORT in run.sh)...")
                        next_print = tick + PROGRESS_S
                elif not gauges:
                    if not answered:
                        info("the exporter answers, but the Zerobus sink gauges are not there yet")
                        answered = True
                    last_seen = tick
                else:
                    if not answered:
                        ok("reading the gauges")
                        answered = True
                        next_print = tick + PROGRESS_S
                    last_seen = tick
                    samples += 1
                    current, last, maximum = gauges["current"], gauges["last"], gauges["max"]
                    max_gauge = maximum
                    if "flushes" in gauges:     # the first value stays, the second follows the last sample
                        if not flushes:
                            flushes.append(gauges["flushes"])
                        flushes[1:] = [gauges["flushes"]]
                    writer.writerow([datetime.now().isoformat(timespec="milliseconds"), int(current), int(last),
                                     int(maximum)])
                    if current > 0:
                        busy += 1
                        peak = max(peak, current) if in_wait else current
                        in_wait = True
                    elif in_wait:
                        waits.append(peak)
                        lasts.append(last)
                        in_wait, peak = False, 0.0
                    if tick >= next_print:
                        next_print = tick + PROGRESS_S
                        now = f"in a wait for {current / 1000:.1f} s" if current > 0 else "between batches"
                        info(f"[{tick - started:5.0f} s] {samples} samples, {len(waits)} waits seen, now {now}, "
                             f"max {maximum / 1000:.1f} s")
                time.sleep(max(0.0, args.every_ms / 1000 - (time.monotonic() - tick)))
        except KeyboardInterrupt:
            print()
            info("stopped with Ctrl+C")

    section("Summary")
    if not samples:
        fail("no sample: start Debezium with DBZ_METRICS_PORT and run this again")
        return summary()
    ok(f"{samples} samples in {time.monotonic() - started:.0f} s; every one in {out.relative_to(ROOT)}")
    if waits:
        ok(f"{len(waits)} waits seen with CurrentFlushDurationMillis above 0: median of the highest sample "
           f"{percentile(waits, 0.5) / 1000:.2f} s, p90 {percentile(waits, 0.9) / 1000:.2f} s, "
           f"longest {max(waits) / 1000:.2f} s")
        gaps = [last - peak for peak, last in zip(waits, lasts, strict=True)]
        info(f"LastFlushDurationMillis after each wait minus its highest sample: median "
             f"{percentile(gaps, 0.5) / 1000:.2f} s (at most one sample interval if the gauge keeps up)")
        info(f"{busy} samples above 0 and {samples - busy} at 0: the gauge goes back to 0 between batches")
        if len(flushes) == 2:
            batches = int(flushes[1] - flushes[0])
            info(f"batches flushed while watching (TotalFlushes): {batches}; when a gap between two batches is "
                 f"shorter than {args.every_ms} ms, two batches look like one wait")
    else:
        warn("no wait seen: CurrentFlushDurationMillis stayed at 0 (batches shorter than the sample interval?)")
    info(f"MaxFlushDurationMillis at the end: {max_gauge / 1000:.2f} s")
    return summary()


if __name__ == "__main__":
    sys.exit(guard(main))
