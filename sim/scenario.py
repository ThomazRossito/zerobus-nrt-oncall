# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
"""The on-call scenario: the shop's services, the scripted timeline and the seeded traffic model.

Scenario time runs from 14:00 to 14:20 (second 0 to 1200). The runner maps it to real time with a speed factor, so
every timestamp written to Postgres is a real instant of the run, while the content of the run (requests, errors,
latencies and events) depends only on the seed.
"""
from __future__ import annotations

import math
import random

SCHEMA = "cmdb"
PUBLICATION = "oncall_cdc"         # the Debezium connector of Phase 2 reads this publication

START_MINUTE = 14 * 60             # scenario second 0 is 14:00 on the narrative clock
DURATION_S = 20 * 60               # 14:00 to 14:20
DEFAULT_SPEED = 12.0               # scenario seconds per real second: one scenario minute every 5 s
DEFAULT_SEED = 42

# Initial state of the service catalog and the traffic model of each service, in this order.
SERVICES = {
    "checkout": {"version": "2.4.0", "owner": "team-checkout", "method": "POST", "route": "/checkout",
                 "per_minute": 300, "latency_ms": 120.0, "error_rate": 0.010},
    "payment": {"version": "3.2.1", "owner": "team-payments", "method": "POST", "route": "/charge",
                "per_minute": 150, "latency_ms": 180.0, "error_rate": 0.005},
    "inventory": {"version": "1.9.0", "owner": "team-inventory", "method": "POST", "route": "/reserve",
                  "per_minute": 200, "latency_ms": 60.0, "error_rate": 0.003},
}

# checkout 2.4.1 has a bug: its error rate grows from the base rate to 12% in the five minutes after it goes live,
# so the first full minute above 5% is 14:04 to 14:05
BAD_RELEASE = ("checkout", "2.4.1")
BAD_PEAK_ERROR_RATE = 0.12
BAD_RAMP_S = 5 * 60

ERROR_MESSAGES = {
    "checkout": "checkout failed: upstream timeout",
    "payment": "charge failed: card processor timeout",
    "inventory": "reserve failed: stock service timeout",
}
BAD_RELEASE_MESSAGE = "checkout failed: discount rule not found for coupon"

# The script of the incident, in scenario seconds. Each entry: (second, action, details).
TIMELINE = (
    (2 * 60, "deploy", {"service": "checkout", "version": "2.4.1", "record": False}),
    (5 * 60, "incident_open", {"key": "main", "service": "checkout", "severity": "sev2",
                               "title": "Checkout error rate above 5%"}),
    (6 * 60, "incident_open", {"key": "duplicate", "service": "checkout", "severity": "sev3",
                               "title": "Checkout 5xx responses above threshold"}),
    (7 * 60, "incident_delete", {"key": "duplicate"}),
    (7 * 60, "incident_status", {"key": "main", "status": "investigating"}),
    (10 * 60, "catalog_record", {"service": "checkout", "version": "2.4.1", "deployed_at_s": 2 * 60}),
    (10 * 60, "incident_suspect", {"key": "main"}),
    (14 * 60, "deploy", {"service": "checkout", "version": "2.4.0", "record": True, "rollback": True}),
    (16 * 60, "incident_status", {"key": "main", "status": "mitigated"}),
    (20 * 60, "incident_status", {"key": "main", "status": "resolved"}),
)


def clock(second: int) -> str:
    """Narrative clock of a scenario second, HH:MM."""
    minute = START_MINUTE + second // 60
    return f"{minute // 60:02d}:{minute % 60:02d}"


def error_rate(service: str, version: str, second: int, live_since: int) -> float:
    """Probability that a request fails, for the version live at that second."""
    base = SERVICES[service]["error_rate"]
    if (service, version) != BAD_RELEASE:
        return base
    progress = min(1.0, max(0.0, (second - live_since) / BAD_RAMP_S))
    return base + (BAD_PEAK_ERROR_RATE - base) * progress


def poisson(rng: random.Random, mean: float) -> int:
    """Number of requests in one scenario second (Knuth's method; fine for small means)."""
    limit, k, product = math.exp(-mean), 0, 1.0
    while True:
        product *= rng.random()
        if product <= limit:
            return k
        k += 1


def latency_ms(rng: random.Random, median_ms: float) -> float:
    """Log-normal latency around the service median."""
    return median_ms * math.exp(rng.gauss(0.0, 0.35))
