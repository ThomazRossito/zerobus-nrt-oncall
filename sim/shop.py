# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
"""The fictitious shop: checkout, payment and inventory, instrumented with the OpenTelemetry SDK.

Each (service, version) gets its own providers, because service.version is a resource attribute. In Phase 1 the
exporters write JSON lines to files in the run folder; Phase 2 swaps them for OTLP to the OTel Collector.
"""
from __future__ import annotations

import math
import random
import threading
from pathlib import Path

from opentelemetry import trace
from opentelemetry._logs import SeverityNumber
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import ConsoleLogRecordExporter, SimpleLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode

from sim.scenario import (BAD_RELEASE, BAD_RELEASE_MESSAGE, ERROR_MESSAGES, SERVICES, error_rate, latency_ms,
                          poisson)

SCOPE = "zerobus-nrt-oncall.shop"
# bucket boundaries the HTTP semantic conventions suggest for http.server.request.duration, in seconds
DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0)


def _line(item) -> str:
    return item.to_json(indent=None) + "\n"


class _LockedFile:
    """One file shared by several exporters; the metric readers write from their own threads."""

    def __init__(self, path: Path):
        self.path = path
        self._file = path.open("w", encoding="utf-8")
        self._lock = threading.Lock()

    def write(self, text: str) -> None:
        with self._lock:
            self._file.write(text)

    def flush(self) -> None:
        with self._lock:
            self._file.flush()

    def close(self) -> None:
        with self._lock:
            self._file.close()


class _Instruments:
    def __init__(self, service: str, version: str, files: dict[str, _LockedFile], metric_interval_ms: float):
        resource = Resource.create({"service.name": service, "service.version": version, "service.namespace": "shop",
                                    "deployment.environment.name": "demo"})
        self.tracer_provider = TracerProvider(resource=resource, shutdown_on_exit=False)
        self.tracer_provider.add_span_processor(
            SimpleSpanProcessor(ConsoleSpanExporter(out=files["spans"], formatter=_line)))
        reader = PeriodicExportingMetricReader(ConsoleMetricExporter(out=files["metrics"], formatter=_line),
                                               export_interval_millis=metric_interval_ms)
        self.meter_provider = MeterProvider(resource=resource, metric_readers=[reader], shutdown_on_exit=False)
        self.logger_provider = LoggerProvider(resource=resource, shutdown_on_exit=False)
        self.logger_provider.add_log_record_processor(
            SimpleLogRecordProcessor(ConsoleLogRecordExporter(out=files["logs"], formatter=_line)))
        self.tracer = self.tracer_provider.get_tracer(SCOPE)
        self.duration = self.meter_provider.get_meter(SCOPE).create_histogram(
            "http.server.request.duration", unit="s", description="Duration of HTTP server requests.",
            explicit_bucket_boundaries_advisory=DURATION_BUCKETS)
        self.logger = self.logger_provider.get_logger(SCOPE)

    def shutdown(self) -> None:
        self.tracer_provider.shutdown()
        self.meter_provider.shutdown()
        self.logger_provider.shutdown()


def _percentile(sorted_values: list[float], fraction: float) -> float:
    return sorted_values[max(0, math.ceil(fraction * len(sorted_values)) - 1)]


class Shop:
    """Generates the requests of each scenario second and keeps the per-minute numbers of the run."""

    def __init__(self, folder: Path, metric_interval_s: float):
        self._files = {signal: _LockedFile(folder / f"{signal}.jsonl") for signal in ("spans", "metrics", "logs")}
        self._metric_interval_ms = metric_interval_s * 1000
        self._instruments: dict[tuple[str, str], _Instruments] = {}
        self.live = {service: spec["version"] for service, spec in SERVICES.items()}
        self.live_since = {service: 0 for service in SERVICES}
        self._minute: dict[tuple[str, str], list[tuple[float, bool]]] = {}
        # expected failures not yet turned into failed requests: keeps each minute's error rate on the curve
        self._error_budget = {service: 0.0 for service in SERVICES}
        self.counts = {"spans": 0, "logs": 0}

    def _get(self, service: str, version: str) -> _Instruments:
        key = (service, version)
        if key not in self._instruments:
            self._instruments[key] = _Instruments(service, version, self._files, self._metric_interval_ms)
        return self._instruments[key]

    def deploy(self, service: str, version: str, second: int, at_ns: int) -> None:
        """A new version goes live; the telemetry from now on carries it as service.version."""
        self.live[service], self.live_since[service] = version, second
        self._get(service, version).logger.emit(
            timestamp=at_ns, severity_number=SeverityNumber.INFO, severity_text="INFO",
            body=f"deploy: {service} {version} is live", attributes={"deployment.status": "succeeded"})
        self.counts["logs"] += 1

    def serve(self, second: int, rng: random.Random, start_ns: int, tick_ns: int) -> None:
        """All requests of one scenario second, spread over its slice of real time.

        How many requests fail follows the error-rate curve; which ones fail is drawn from the seed.
        """
        for service, spec in SERVICES.items():
            version = self.live[service]
            instruments = self._get(service, version)
            failure_rate = error_rate(service, version, second, self.live_since[service])
            message = BAD_RELEASE_MESSAGE if (service, version) == BAD_RELEASE else ERROR_MESSAGES[service]
            requests = poisson(rng, spec["per_minute"] / 60)
            self._error_budget[service] += failure_rate * requests
            errors = min(requests, int(self._error_budget[service]))
            self._error_budget[service] -= errors
            failing = set(rng.sample(range(requests), errors)) if errors else set()
            for i in range(requests):
                failed = i in failing
                duration_ms = latency_ms(rng, spec["latency_ms"]) * (0.6 if failed else 1.0)
                begin = start_ns + i * tick_ns // requests
                attributes = {"http.request.method": spec["method"], "http.route": spec["route"],
                              "http.response.status_code": 500 if failed else 200}
                if failed:
                    attributes["error.type"] = "500"
                span = instruments.tracer.start_span(f"{spec['method']} {spec['route']}", kind=SpanKind.SERVER,
                                                     start_time=begin, attributes=attributes)
                if failed:
                    span.set_status(Status(StatusCode.ERROR, message))
                    instruments.logger.emit(timestamp=begin, context=trace.set_span_in_context(span),
                                            severity_number=SeverityNumber.ERROR, severity_text="ERROR",
                                            body=message, attributes=attributes)
                    self.counts["logs"] += 1
                span.end(end_time=begin + int(duration_ms * 1_000_000))
                instruments.duration.record(duration_ms / 1000, attributes=attributes)
                self.counts["spans"] += 1
                self._minute.setdefault((service, version), []).append((duration_ms, failed))

    def close_minute(self, minute: int) -> list[dict]:
        """Numbers of one scenario minute per (service, version), in scenario time only."""
        rows = []
        for (service, version), samples in self._minute.items():
            durations = sorted(duration for duration, _ in samples)
            errors = sum(1 for _, failed in samples if failed)
            rows.append({"t": minute * 60, "kind": "minute", "service": service, "version": version,
                         "requests": len(samples), "errors": errors,
                         "p50_ms": round(_percentile(durations, 0.50), 1),
                         "p95_ms": round(_percentile(durations, 0.95), 1)})
        self._minute = {}
        return rows

    def shutdown(self) -> int:
        """Flushes and closes everything; returns how many metric exports were written."""
        for instruments in self._instruments.values():
            instruments.shutdown()
        for handle in self._files.values():
            handle.close()
        with self._files["metrics"].path.open(encoding="utf-8") as metrics:
            return sum(1 for _ in metrics)
