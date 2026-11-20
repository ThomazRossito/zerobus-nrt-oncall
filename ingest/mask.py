#!/usr/bin/env python3
# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
"""Filter for the console of the ingestion processes (Debezium Server now, the OTel Collector later).

Reads lines from stdin and writes them to stdout with URLs, workspace hosts, e-mails and IDs masked (the same rules
as the setup scripts), plus the Postgres user, role and host, the machine's user name, host name, machine ID and
temporary folder, and the home folder, so what shows in the terminal can be pasted in a chat. Lines longer than
LONG characters (the Java class path of the configuration dump) are cut. The run scripts keep the full log in a file.

Ignores Ctrl+C: the process before it in the pipe gets the signal, shuts down and closes the pipe, and its last
lines still go through. Reads no environment variable besides HOME.
"""
from __future__ import annotations

import os
import re
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.common import scrub  # noqa: E402

LONG = 1500
# a value ends at whitespace, a quote, a backslash (an escaped newline in a JSON log line) or a closing bracket
VALUE = r"[^\s\"'\\\])>,]+"
PATTERNS = [
    (re.compile(r"https?://" + VALUE), "<url>"),
    # the connector configuration Debezium prints at start, and the JVM properties that come with it
    (re.compile(r"\b((?:database\.user|database\.hostname|user\.name|io\.netty\.machineId|java\.io\.tmpdir)\s*[=:]\s*)"
                + VALUE), r"\1<masked>"),
    (re.compile(r'("hostName"\s*:\s*")[^"]*'), r"\1<masked>"),
    # also after an escaped tab or newline of a JSON log line (\\trole 'name')
    (re.compile(r"(?:(?<=\\[tn])|(?<![A-Za-z0-9_]))(user|role) '[^']*'"), r"\1 '<masked>'"),
    (re.compile(r"/var/folders/[^\s\"'\\]+"), "/var/folders/<masked>"),
    # a home folder other than this shell's (another user, or HOME not set)
    (re.compile(r"(?<![\w.])/(?:Users|home)/[^/\s\"'\\]+"), "~"),
]


def mask(line: str, home: str) -> str:
    if len(home) > 1:
        line = line.replace(home, "~")
    for pattern, replacement in PATTERNS:
        line = pattern.sub(replacement, line)
    line = scrub(line)
    if len(line) > LONG:
        cut = len(line) - 300
        line = f"{line[:300].rstrip()} ... ({cut:,} characters cut here; the full line is in the log file)\n"
    return line


def main() -> int:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    sys.stdin.reconfigure(errors="replace")
    home = os.environ.get("HOME", "")
    for line in sys.stdin:
        sys.stdout.write(mask(line, home))
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
