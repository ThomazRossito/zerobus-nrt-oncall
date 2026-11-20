# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
"""Helpers shared by the zerobus-nrt-oncall scripts: .env, output safe to paste in a chat, Databricks and SQL."""
from __future__ import annotations

import json
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COUNTS = {"ok": 0, "warn": 0, "fail": 0}

# no Python script reads the OAuth secret: only Debezium Server and the OTel Collector get it
NEVER_READ = {"DATABRICKS_CLIENT_SECRET"}

# what must never reach the screen: URLs, workspace host, e-mails and IDs
_SENSITIVE = [
    (re.compile(r"https?://\S+"), "<url>"),
    (re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+"), "<e-mail>"),
    (re.compile(r"\b[\w.-]+\.(?:azuredatabricks\.net|cloud\.databricks\.com|gcp\.databricks\.com)\b"), "<host>"),
    (re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I), "<id>"),
    (re.compile(r"\b[0-9a-f]{16,}\b", re.I), "<id>"),
    (re.compile(r"\b\d{10,}\b"), "<id>"),
]


def scrub(text: object) -> str:
    """Removes URLs, hosts, e-mails and IDs from text that came from outside (API errors, CLI output)."""
    text = str(text)
    for pattern, replacement in _SENSITIVE:
        text = pattern.sub(replacement, text)
    return text


def first_line(text: object) -> str:
    for line in str(text or "").splitlines():
        if line.strip():
            return line.strip()
    return ""


# ---- output -------------------------------------------------------------------------------------------

def section(title: str) -> None:
    print(f"\n== {title}")


def ok(msg: str) -> None:
    COUNTS["ok"] += 1
    print(f"  ok     {msg}")


def warn(msg: str) -> None:
    COUNTS["warn"] += 1
    print(f"  warn   {msg}")


def fail(msg: str) -> None:
    COUNTS["fail"] += 1
    print(f"  FAIL   {msg}")


def info(msg: str) -> None:
    print(f"         {msg}")


def summary() -> int:
    print(f"\nsummary: {COUNTS['ok']} ok, {COUNTS['warn']} warning(s), {COUNTS['fail']} failure(s)")
    return 1 if COUNTS["fail"] else 0


# ---- .env and local state -------------------------------------------------------------------------------

def read_env() -> dict[str, str]:
    """Reads .env from the project root, without expanding variables and without keeping the OAuth secret."""
    path = ROOT / ".env"
    if not path.exists():
        print(".env not found in the project root: cp .env.example .env and fill it in")
        sys.exit(1)
    env: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().removeprefix("export ").strip()
        if key in NEVER_READ:
            continue
        env[key] = value.strip().strip('"').strip("'")
    return env


def missing(env: dict[str, str], keys: tuple[str, ...]) -> list[str]:
    return [k for k in keys if not env.get(k)]


def save_local(name: str, data: dict) -> Path:
    """Writes to .local/ (outside git) what one phase measures and the next one uses."""
    folder = ROOT / ".local"
    folder.mkdir(exist_ok=True)
    path = folder / name
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


# ---- commands, Databricks and SQL ------------------------------------------------------------------------

def run(cmd: list[str], cwd: Path | None = None, timeout: int = 120) -> tuple[int, str, str]:
    """Runs a command and returns (exit code, stdout, stderr), printing nothing."""
    try:
        p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, p.stdout, p.stderr
    except FileNotFoundError:
        return 127, "", f"{cmd[0]}: not found"
    except subprocess.TimeoutExpired:
        return 124, "", f"{cmd[0]}: took longer than {timeout} s"


def client(env: dict[str, str]):
    """WorkspaceClient for the CLI profile set in .env."""
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient(profile=env.get("DATABRICKS_CONFIG_PROFILE") or None)


class SqlFailed(Exception):
    pass


def sql(w, warehouse_id: str, statement: str, wait_s: int = 300) -> list[list]:
    """Runs a statement on the SQL warehouse and returns the rows (lists of strings). Waits for the warehouse to start."""
    from databricks.sdk.service.sql import StatementState
    r = w.statement_execution.execute_statement(statement=statement, warehouse_id=warehouse_id, wait_timeout="30s")
    deadline = time.monotonic() + wait_s
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.monotonic() > deadline:
            w.statement_execution.cancel_execution(r.statement_id)
            raise SqlFailed(f"took longer than {wait_s} s")
        time.sleep(1)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        error = r.status.error.message if r.status.error else r.status.state.value
        raise SqlFailed(scrub(first_line(error)))
    return (r.result.data_array or []) if r.result else []
