#!/usr/bin/env python3
# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-24
#
# -----------------------------------------------------------------------------
"""Phase 4 · the Genie Agent of the on-call scenario: deploys it with the bundle, then asks it, and Genie One, the
postmortem question.

Usage, from the project root with the conda env active (conda activate zerobus-nrt-oncall):
  python setup/04_genie.py check              read-only: the agent's tables and the answer they hold, the warehouse,
                                              the CLI, bundle validate and, once deployed, the agent
  python setup/04_genie.py run                checks the inputs, deploys the bundle and asks the agent the postmortem
                                              question through the Genie Agents API
  python setup/04_genie.py ask ["QUESTION"]   asks Genie One (databricks genie ask) the postmortem question, or
                                              QUESTION, and runs again the SQL it ran to check the two versions

The agent is bundle/resources/oncall.genie.yml. Each answer is saved to .local/phase4_<agent|genie_one>-<time>.json.
Reads from .env: DATABRICKS_CONFIG_PROFILE, UC_CATALOG and WAREHOUSE_ID. Never reads DATABRICKS_CLIENT_SECRET: it
removes it and DATABRICKS_CLIENT_ID from this process, unread, so the CLI and the SDK use the profile even in a
terminal that sourced .env. The output shows no host, e-mail or IDs, so it is safe to paste in a chat.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import textwrap
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.bundle import BUNDLE, bundle_variables, deploy, error_line, resource_id  # noqa: E402
from lib.common import (ROOT, SqlFailed, client, fail, first_line, guard, info, missing, ok, read_env,  # noqa: E402
                        run, save_local, scrub, section, sql, summary, warn)
from sim.scenario import BAD_RELEASE, SERVICES, TIMELINE  # noqa: E402

NEEDED = ("DATABRICKS_CONFIG_PROFILE", "UC_CATALOG", "WAREHOUSE_ID")
# exported for Debezium by "set -a; source .env": the CLI and the SDK read them too and would mix them with the profile
SERVICE_PRINCIPAL = ("DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET")
AGENT = "oncall_genie"            # resource key in oncall.genie.yml; keys are unique across all resources
TABLES = ("gold.errors_by_version", "gold.hop_latency", "gold.incident_postmortem", "gold.service_health",
          "silver.services_bitemporal")              # the agent's data sources, as in oncall.genie.yml
CLI_MIN = (1, 3, 0)                                  # genie_spaces in bundles
READ_ONLY = re.compile(r"^\s*(?:--[^\n]*\n\s*)*(SELECT|WITH)\b", re.IGNORECASE)
SHOWN = {"text": 30, "sql": 25, "rows": 5}           # lines of each part of an answer printed here
EMBEDDED = re.compile(r"<!-- begin-embedded:(\S+) -->(.*?)<!-- end-embedded:\1 -->", re.DOTALL)

# the scenario (sim/scenario.py): the release that broke checkout, and the incident of the postmortem
SERVICE, BAD_VERSION = BAD_RELEASE                   # checkout, 2.4.1: live when the incident opened
GOOD_VERSION = SERVICES[SERVICE]["version"]          # 2.4.0: what the catalog named at that moment
TITLE = next(d["title"] for _, action, d in TIMELINE if action == "incident_open" and d["key"] == "main")
QUESTION = (f"Which {SERVICE} version was live when the latest '{TITLE}' incident opened, "
            "and which version did the service catalog name at that moment?")


def literal(text: str) -> str:
    """A SQL string literal for a value that comes from sim/scenario.py."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def utc(ms) -> str:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


class Query:
    """Runs statements on the SQL warehouse, with the project catalog as the default catalog."""

    def __init__(self, w, env: dict[str, str]):
        self.w, self.warehouse, self.catalog = w, env["WAREHOUSE_ID"], env["UC_CATALOG"]

    def __call__(self, statement: str) -> list[list]:
        return sql(self.w, self.warehouse, statement, catalog=self.catalog)


def connect(env: dict[str, str]):
    section("Databricks")
    if keys := missing(env, NEEDED):
        fail("missing in .env: " + ", ".join(keys))
        return None
    for name in SERVICE_PRINCIPAL:
        os.environ.pop(name, None)
    try:
        w = client(env)
        w.current_user.me()
    except Exception as error:  # noqa: BLE001
        fail(f"profile {env['DATABRICKS_CONFIG_PROFILE']}: {scrub(first_line(error))}")
        return None
    ok(f"profile {env['DATABRICKS_CONFIG_PROFILE']}: authenticated")
    return w


# ---- inputs ------------------------------------------------------------------------------------------------

def warehouse(w, env: dict[str, str]) -> bool:
    section("SQL warehouse")
    try:
        wh = w.warehouses.get(env["WAREHOUSE_ID"])
    except Exception as error:  # noqa: BLE001
        fail(f"WAREHOUSE_ID: {scrub(first_line(error))}")
        return False
    kind = getattr(wh.warehouse_type, "value", None) or "?"
    if wh.enable_serverless_compute:
        ok("serverless warehouse: Genie Agents need a pro or serverless one")
        return True
    if kind == "PRO":
        ok("pro warehouse: Genie Agents need a pro or serverless one")
        return True
    fail(f"{kind.lower()} warehouse: Genie Agents need a pro or serverless SQL warehouse")
    return False


def tables(q: Query) -> bool:
    section("The agent's tables (Phase 3)")
    good = True
    for name in TABLES:
        try:
            count = int(q(f"SELECT count(*) FROM {name}")[0][0])
        except SqlFailed as error:
            fail(f"{name}: {error}")
            good = False
            continue
        (ok if count else fail)(f"{name}: {count:,} rows")
        good = good and count > 0
    if not good:
        info("the tables come from the Phase 3 pipeline: python setup/03_pipeline.py run")
    return good


def expected(q: Query) -> bool:
    """The postmortem answer the tables hold, for the latest incident of the scenario."""
    section("The postmortem answer in the tables")
    try:
        rows = q(f"SELECT incident_id, unix_millis(opened_at), catalog_live_at_open, catalog_known_at_open, "
                 f"count(*) OVER () FROM gold.incident_postmortem "
                 f"WHERE service = {literal(SERVICE)} AND title = {literal(TITLE)} "
                 "ORDER BY opened_at DESC LIMIT 1")
    except SqlFailed as error:
        fail(f"gold.incident_postmortem: {error}")
        return False
    if not rows:
        fail(f'no incident "{TITLE}" in gold.incident_postmortem: python setup/03_pipeline.py run')
        return False
    incident_id, opened_ms, live, known, same_title = rows[0]
    info(f'{same_title} incident(s) titled "{TITLE}"; the latest, #{incident_id}, opened {utc(opened_ms)}')
    right = (live, known) == (BAD_VERSION, GOOD_VERSION)
    (ok if right else fail)(f"live when it opened: {live or '-'} (expected {BAD_VERSION}); "
                            f"the catalog named: {known or '-'} (expected {GOOD_VERSION})")
    return right


def cli() -> bool:
    section("Databricks CLI")
    rc, out, err = run(["databricks", "--version"])
    if rc != 0:
        fail(f"databricks --version: {scrub(error_line(err or out))}")
        return False
    found = re.search(r"(\d+)\.(\d+)\.(\d+)", out)
    version = tuple(int(n) for n in found.groups()) if found else (0, 0, 0)
    (ok if version >= CLI_MIN else fail)(f"Databricks CLI {'.'.join(map(str, version))} "
                                         f"(genie_spaces in bundles needs {'.'.join(map(str, CLI_MIN))} or later)")
    rc, out, err = run(["databricks", "genie", "ask", "--help"])
    (ok if rc == 0 else fail)("databricks genie ask: " + ("available" if rc == 0 else scrub(error_line(err or out))))
    return version >= CLI_MIN and rc == 0


def validate(env: dict[str, str]) -> bool:
    """databricks bundle validate with the variables of .env: the agent's YAML and serialized_space, before a deploy."""
    section("Bundle")
    variables = [item for key, value in bundle_variables(env).items() for item in ("--var", f"{key}={value}")]
    rc, out, err = run(["databricks", "bundle", "validate", *variables, "-p", env["DATABRICKS_CONFIG_PROFILE"]],
                       cwd=BUNDLE, timeout=300)
    if rc != 0:
        fail(f"bundle validate: {scrub(error_line(err or out))}")
        return False
    warnings = [line.strip() for line in f"{out}\n{err}".splitlines() if line.strip().lower().startswith("warning")]
    for line in warnings[:5]:
        warn(f"bundle validate: {scrub(line)}")
    ok("bundle validate: the pipeline and the Genie Agent")
    return True


def agent(env: dict[str, str], w) -> str | None:
    """The deployed agent, from bundle summary. Its ID is used, never printed."""
    section("The agent")
    space_id = resource_id(env, bundle_variables(env), "genie_spaces", AGENT, quiet=True)
    if space_id is None:
        info("not deployed yet: python setup/04_genie.py run")
        return None
    try:
        space = w.genie.get_space(space_id, include_serialized_space=True)
        sources = json.loads(space.serialized_space or "{}").get("data_sources", {}).get("tables", [])
    except Exception as error:  # noqa: BLE001
        fail(f"agent {AGENT}: {scrub(first_line(error))}")
        return None
    ok(f'agent "{space.title}" in the workspace, {len(sources)} tables')
    return space_id


# ---- answers -----------------------------------------------------------------------------------------------

def rows_of(q: Query, statement: str) -> list[dict] | None:
    """Runs again a read-only statement that Genie ran, each row as a dict with its column names."""
    if not READ_ONLY.match(statement or ""):
        warn("the SQL does not start with SELECT or WITH: not run here")
        return None
    body = statement.strip()
    while True:                                  # drop a trailing comment and semicolon: the SQL becomes a subquery
        cleaned = re.sub(r"\s*--[^'\"\n]*$", "", body).rstrip().rstrip(";").rstrip()
        if cleaned == body:
            break
        body = cleaned
    try:
        # the newlines keep a trailing comment of Genie's SQL from swallowing the closing parenthesis
        return [json.loads(row[0]) for row in q(f"SELECT to_json(struct(*)) FROM (\n{body}\n)")]
    except (SqlFailed, ValueError) as error:
        warn(f"running the SQL again: {scrub(first_line(error))}")
        return None


def embedded_rows(text: str) -> list[dict]:
    """The result tables Genie One embeds in its answer, between <!-- begin-embedded:... --> and <!-- end-embedded -->.

    When Genie One hands the question to a Genie Agent (ask_genie_space), the SQL runs inside the agent and does not
    come back in tool_calls; the result table in the answer, with its column names, is what is left to check.
    """
    rows = []
    for _, table in EMBEDDED.findall(text or ""):
        lines = [line.strip() for line in table.splitlines() if line.strip().startswith("|")]
        if len(lines) < 3:
            continue
        header = [cell.strip(" *`") for cell in lines[0].strip("|").split("|")]
        for line in lines[2:]:
            cells = [cell.strip(" *`") for cell in line.strip("|").split("|")]
            if len(cells) == len(header):
                rows.append(dict(zip(header, cells)))
    return rows


def judge(q: Query, who: str, text: str, queries: list[dict]) -> dict:
    """Prints the answer, runs its SQL again and checks the two versions. Returns what is saved to .local/."""
    if text:
        lines = [line for part in scrub(text).splitlines() for line in (textwrap.wrap(part, 100) or [""])]
        info("answer:")
        for line in lines[:SHOWN["text"]]:
            info("  " + line)
        if len(lines) > SHOWN["text"]:
            info(f"  ... ({len(lines) - SHOWN['text']} more lines in the .local file)")
    rows: list[dict] = []
    for n, query in enumerate(queries, 1):
        title = f" ({scrub(query['title'])})" if query.get("title") else ""
        info(f"SQL {n}{title}:")
        sql_lines = scrub(query["sql"]).strip().splitlines()
        for line in sql_lines[:SHOWN["sql"]]:
            info("  " + line)
        if len(sql_lines) > SHOWN["sql"]:
            info(f"  ... ({len(sql_lines) - SHOWN['sql']} more lines)")
        query["rows"] = rows_of(q, query["sql"])
        if query["rows"] is not None:
            info(f"  {len(query['rows'])} row(s) when run again here:")
            for row in query["rows"][:SHOWN["rows"]]:
                info("  " + scrub(json.dumps(row, ensure_ascii=False))[:200])
            rows += query["rows"]
    table_rows = embedded_rows(text)
    if table_rows:
        info(f"{len(table_rows)} row(s) in the result table of the answer")
        rows += table_rows
    # with the gold columns, the check knows which version is which; otherwise both versions must be in the result
    keyed = [r for r in rows if {"catalog_live_at_open", "catalog_known_at_open"} <= set(r)]
    if keyed:
        right = any(str(r["catalog_live_at_open"]) == BAD_VERSION and str(r["catalog_known_at_open"]) == GOOD_VERSION
                    for r in keyed)
        (ok if right else fail)(f"in the result: catalog_live_at_open {BAD_VERSION}, catalog_known_at_open {GOOD_VERSION}")
    elif rows:
        right = {BAD_VERSION, GOOD_VERSION} <= {str(v) for r in rows for v in r.values()}
        (ok if right else fail)(f"the result has {BAD_VERSION} and {GOOD_VERSION}")
        info(f"read it: {BAD_VERSION} must be the version that was live, {GOOD_VERSION} the one the catalog named")
    else:
        right = None
        warn(f"{who} returned no SQL and no result table to check: read the answer")
    in_text = BAD_VERSION in text and GOOD_VERSION in text
    (ok if in_text else fail)(f"the answer names {BAD_VERSION} and {GOOD_VERSION}")
    return {"text": text, "queries": queries, "table_rows": table_rows, "right_in_result": right,
            "versions_in_text": in_text}


def ask_agent(w, q: Query, space_id: str, question: str) -> None:
    section("The agent, asked through the Genie Agents API")
    info(f"question: {question}")
    started = time.monotonic()
    try:
        message = w.genie.start_conversation_and_wait(space_id, question, timeout=timedelta(minutes=10))
    except Exception as error:  # noqa: BLE001
        fail(f"the agent did not answer: {scrub(first_line(error))}")
        return
    seconds = time.monotonic() - started
    status = getattr(message.status, "value", None) or str(message.status)
    (ok if status == "COMPLETED" else fail)(f"answer in {seconds:.0f} s, status {status}")
    if message.error is not None:
        warn(f"error: {scrub(first_line(getattr(message.error, 'error', message.error)))}")
    texts, queries = [], []
    for attachment in message.attachments or []:
        if attachment.text is not None and attachment.text.content:
            texts.append(attachment.text.content)
        if attachment.query is not None and attachment.query.query:
            queries.append({"title": attachment.query.title or attachment.query.description,
                            "sql": attachment.query.query})
    record = judge(q, "the agent", "\n\n".join(texts), queries)
    record.update({"question": question, "status": status, "seconds": round(seconds, 1)})
    saved = save_local(f"phase4_agent-{datetime.now():%Y%m%d-%H%M%S}.json", record)
    info(f"saved to {saved.relative_to(ROOT)}")


def ask_genie_one(env: dict[str, str], q: Query, question: str) -> None:
    section("Genie One, asked through databricks genie ask")
    info(f"question: {question}")
    session = f"zerobus-nrt-oncall-{datetime.now():%Y%m%d-%H%M%S}"   # a new conversation each time, as the docs advise
    started = time.monotonic()
    rc, out, err = run(["databricks", "genie", "ask", question, "-s", session, "--warehouse-id", env["WAREHOUSE_ID"],
                        "-o", "json", "-p", env["DATABRICKS_CONFIG_PROFILE"]], cwd=ROOT, timeout=900)
    seconds = time.monotonic() - started
    if rc != 0:
        fail(f"databricks genie ask: {scrub(error_line(err or out))}")
        return
    try:
        answer = json.loads(out)
    except ValueError:
        fail(f"databricks genie ask: the output is not JSON ({scrub(first_line(out))})")
        return
    status = str(answer.get("status") or "?")
    (ok if status.lower() == "completed" else fail)(f"answer in {seconds:.0f} s, status {status}")
    calls = [c for c in answer.get("tool_calls") or [] if isinstance(c, dict)]
    tools = sorted({str(c.get("name")) for c in calls if c.get("name")})
    info("tools Genie One called: " + (", ".join(tools) if tools else "none listed"))
    if "ask_genie_space" in tools:
        ok("Genie One handed the question to a Genie Agent (ask_genie_space): the SQL ran inside the agent")
    queries = [{"title": c.get("title"), "sql": c["sql"]} for c in calls if c.get("sql")]
    record = judge(q, "Genie One", str(answer.get("text") or ""), queries)
    record.update({"question": question, "status": status, "seconds": round(seconds, 1), "tools": tools})
    saved = save_local(f"phase4_genie_one-{datetime.now():%Y%m%d-%H%M%S}.json", record)
    info(f"saved to {saved.relative_to(ROOT)}")


# ---- actions -----------------------------------------------------------------------------------------------

def check(env: dict[str, str]) -> int:
    w = connect(env)
    if w is None:
        return summary()
    q = Query(w, env)
    warehouse(w, env)
    tables(q)
    expected(q)
    if cli() and validate(env):
        agent(env, w)
    return summary()


def run_agent(env: dict[str, str]) -> int:
    w = connect(env)
    if w is None:
        return summary()
    q = Query(w, env)
    inputs = [warehouse(w, env), tables(q), expected(q), cli()]
    if not all(inputs):
        info("nothing was deployed: the inputs above have to pass first")
        return summary()
    section("Bundle")
    info("bundle deploy: about a minute")
    if not deploy(env, bundle_variables(env)):
        return summary()
    space_id = agent(env, w)
    if space_id is None:
        fail(f"the bundle deployed, but agent {AGENT} is not in the bundle summary")
        return summary()
    ask_agent(w, q, space_id, QUESTION)
    return summary()


def ask(env: dict[str, str], question: str | None) -> int:
    w = connect(env)
    if w is None:
        return summary()
    q = Query(w, env)
    if not expected(q):
        return summary()
    ask_genie_one(env, q, question or QUESTION)
    return summary()


def main() -> int:
    parser = argparse.ArgumentParser(description="zerobus-nrt-oncall · Phase 4")
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("check", help="read-only: tables, expected answer, warehouse, CLI and the deployed agent")
    actions.add_parser("run", help="deploys the bundle and asks the agent the postmortem question")
    one = actions.add_parser("ask", help="asks Genie One the postmortem question, or QUESTION")
    one.add_argument("question", nargs="?", help="another question for Genie One, in quotes")
    args = parser.parse_args()
    env = read_env()
    if args.action == "check":
        return check(env)
    if args.action == "run":
        return run_agent(env)
    return ask(env, args.question)


if __name__ == "__main__":
    sys.exit(guard(main))
