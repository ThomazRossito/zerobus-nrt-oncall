# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-22
#
# -----------------------------------------------------------------------------
"""Declarative Automation Bundle and Lakeflow pipeline helpers for the setup scripts from Phase 3 on: deploy the
bundle, find a pipeline it deployed, run an update and read why it failed, with output safe to paste in a chat.
(setup/00_environment.py keeps its own copy of the update loop, as Phase 0 closed with it.)"""
from __future__ import annotations

import json
import time

from lib.common import ROOT, fail, first_line, info, ok, run, scrub

BUNDLE = ROOT / "bundle"


def _variables(variables: dict[str, str]) -> list[str]:
    return [item for key, value in variables.items() for item in ("--var", f"{key}={value}")]


def error_line(text: str) -> str:
    """The first line of a CLI output that starts with 'error', or its first line."""
    for line in (text or "").splitlines():
        if line.strip().lower().startswith("error"):
            return line.strip()
    return first_line(text)


def deploy(env: dict[str, str], variables: dict[str, str]) -> bool:
    """databricks bundle deploy, with the CLI profile of .env. Prints one line."""
    rc, out, err = run(["databricks", "bundle", "deploy", *_variables(variables), "-p",
                        env["DATABRICKS_CONFIG_PROFILE"]], cwd=BUNDLE, timeout=900)
    if rc != 0:
        fail(f"bundle deploy: {scrub(error_line(err or out))}")
        return False
    ok("bundle deploy")
    return True


def pipeline_id(env: dict[str, str], variables: dict[str, str], key: str) -> str | None:
    """The ID of a pipeline the bundle deployed, from bundle summary. It is used, never printed."""
    rc, out, err = run(["databricks", "bundle", "summary", "-o", "json", *_variables(variables), "-p",
                        env["DATABRICKS_CONFIG_PROFILE"]], cwd=BUNDLE)
    try:
        return json.loads(out)["resources"]["pipelines"][key]["id"]
    except (ValueError, KeyError, TypeError):
        fail(f"pipeline {key} not found in the bundle summary: {scrub(error_line(err or out))}")
        return None


def pipeline_error(w, pipeline_id: str) -> str:
    """The first error of the pipeline's latest events, scrubbed: the cause is usually in the exception."""
    try:
        events = []
        for event in w.pipelines.list_pipeline_events(pipeline_id=pipeline_id, filter="level='ERROR'", max_results=10):
            events.append(event)
            if len(events) >= 10:
                break
    except Exception as error:  # noqa: BLE001
        return f"could not read the events ({scrub(first_line(error))})"
    for event in events:
        if event.error and event.error.exceptions and event.error.exceptions[0].message:
            return scrub(first_line(event.error.exceptions[0].message))
    for event in events:
        if event.message:
            return scrub(first_line(event.message))
    return "no error message in the events"


def run_update(w, pipeline_id: str, full_refresh: bool = False, limit_s: int = 1200) -> bool:
    """Starts an update of the pipeline and waits for it, printing each change of state."""
    try:
        update_id = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=full_refresh).update_id
    except Exception as error:  # noqa: BLE001
        fail(f"could not start the update: {scrub(first_line(error))}")
        return False
    start, previous, state = time.monotonic(), None, "?"
    while True:
        current = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id).update.state
        state = current.value if current else "?"
        if state != previous:
            info(f"{time.monotonic() - start:4.0f} s  {state}")
            previous = state
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            break
        if time.monotonic() - start > limit_s:
            fail(f"the update took longer than {limit_s // 60} min")
            return False
        time.sleep(5)
    if state == "COMPLETED":
        ok(f"update completed in {time.monotonic() - start:.0f} s")
        return True
    fail(f"update {state}: {pipeline_error(w, pipeline_id)}")
    return False
