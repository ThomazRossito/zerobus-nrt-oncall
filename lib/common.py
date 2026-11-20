# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
"""Helpers shared by the zerobus-nrt-oncall scripts: .env, output safe to paste in a chat, Databricks, SQL and
the local Postgres."""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import traceback
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HOME = os.path.expanduser("~")
COUNTS = {"ok": 0, "warn": 0, "fail": 0}

# no Python script reads the OAuth secret: only Debezium Server and the OTel Collector get it
NEVER_READ = {"DATABRICKS_CLIENT_SECRET"}

# what must never reach the screen: URLs, workspace host, e-mails and IDs
_SENSITIVE = [
    (re.compile(r"https?://\S+"), "<url>"),
    # the last label of an e-mail domain has letters: a Go module version such as service@v0.165.0 is not an e-mail
    (re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)*\.[A-Za-z]{2,}\b"), "<e-mail>"),
    (re.compile(r"\b[\w.-]+\.(?:azuredatabricks\.net|cloud\.databricks\.com|gcp\.databricks\.com)\b"), "<host>"),
    (re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I), "<id>"),
    (re.compile(r"\b[0-9a-f]{16,}\b", re.I), "<id>"),
    (re.compile(r"\b\d{10,}\b"), "<id>"),
]


def scrub(text: object) -> str:
    """Removes URLs, hosts, e-mails, IDs and the home folder from text that came from outside (API errors, CLI)."""
    text = str(text)
    if len(HOME) > 1:
        text = text.replace(HOME, "~")
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


def guard(main: Callable[[], int]) -> int:
    """Runs a script's main and turns an error that nothing else caught into one FAIL line.

    A raw traceback shows the workspace host, the home folder and the Python install. The FAIL line keeps the error
    type, the project file and line where it happened and the scrubbed message, so the output can still be pasted.
    """
    try:
        return main()
    except KeyboardInterrupt:
        print("\ninterrupted")
        return 130
    except Exception as error:  # noqa: BLE001 - the last stop before a raw traceback
        ours = [(Path(frame.filename).resolve(), frame.lineno) for frame in traceback.extract_tb(error.__traceback__)]
        ours = [(path, line) for path, line in ours if path.is_relative_to(ROOT)]
        where = f" at {ours[-1][0].relative_to(ROOT)}:{ours[-1][1]}" if ours else ""
        fail(f"{type(error).__name__}{where}: {scrub(first_line(error))}")
        return summary()


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


def sql(w, warehouse_id: str, statement: str, wait_s: int = 300, catalog: str | None = None) -> list[list]:
    """Runs a statement on the SQL warehouse and returns the rows (lists of strings). Waits for the warehouse to start.

    catalog, when given, is the statement's default catalog, so schema.table names resolve inside it.
    Every failure comes back as SqlFailed with a message safe to print, a workspace that does not answer included.
    """
    from databricks.sdk.errors import DatabricksError
    from databricks.sdk.service.sql import StatementState
    try:
        r = w.statement_execution.execute_statement(statement=statement, warehouse_id=warehouse_id,
                                                    wait_timeout="30s", catalog=catalog)
        deadline = time.monotonic() + wait_s
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            if time.monotonic() > deadline:
                w.statement_execution.cancel_execution(r.statement_id)
                raise SqlFailed(f"took longer than {wait_s} s")
            time.sleep(1)
            r = w.statement_execution.get_statement(r.statement_id)
    # DatabricksError is an OSError too, so it comes first: an API error keeps its (scrubbed) message
    except DatabricksError as error:
        raise SqlFailed(scrub(first_line(error))) from None
    # timeouts and refused connections (requests' errors are OSError); their text names the workspace host
    except OSError as error:
        raise SqlFailed(f"the workspace did not answer ({type(error).__name__}); try again in a minute") from None
    if r.status.state != StatementState.SUCCEEDED:
        error = r.status.error.message if r.status.error else r.status.state.value
        raise SqlFailed(scrub(first_line(error)))
    return (r.result.data_array or []) if r.result else []


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


# ---- local Postgres --------------------------------------------------------------------------------------

def pg_connect(env: dict[str, str], dbname: str | None = None, replication_user: bool = False):
    """Connection to the local Postgres in autocommit: each statement is its own commit, the way CDC sees it.

    Uses PG_USER_ADMIN by default; replication_user=True logs in as PG_USER, the user Debezium connects with.
    """
    import psycopg
    user, password = ("PG_USER", "PG_PASSWORD") if replication_user else ("PG_USER_ADMIN", "PG_PASSWORD_ADMIN")
    return psycopg.connect(host=env["PG_HOST"], port=int(env["PG_PORT"]), user=env[user], password=env[password],
                           dbname=dbname or env["PG_DBNAME"], connect_timeout=5, autocommit=True)


def pg_error(error: object, env: dict[str, str]) -> str:
    """First line of a Postgres error, without host, user names or passwords."""
    text = scrub(first_line(error))
    keys = ("PG_PASSWORD_ADMIN", "PG_PASSWORD", "PG_USER_ADMIN", "PG_USER", "PG_HOST")
    values = [(env.get(key), key) for key in keys]
    for value, key in sorted((v for v in values if v[0]), key=lambda v: len(v[0]), reverse=True):
        text = text.replace(value, "<hidden>" if "PASSWORD" in key else f"<{key}>")
    return text
