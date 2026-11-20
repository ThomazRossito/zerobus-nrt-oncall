# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
"""The two bots that write to the local Postgres, one commit per change (the connection is in autocommit).

DeployBot keeps the service catalog (cmdb.services); in the scenario it learns about 2.4.1 eight minutes late.
IncidentBot opens, updates and deletes incidents (cmdb.incidents), reading the catalog to say which version it blames.
"""
from __future__ import annotations

from datetime import datetime


class DeployBot:
    def __init__(self, conn):
        self._conn = conn

    def catalog_version(self, service: str) -> str | None:
        row = self._conn.execute("SELECT version FROM cmdb.services WHERE service = %s", (service,)).fetchone()
        return row[0] if row else None

    def record(self, service: str, version: str, deployed_at: datetime) -> None:
        """Writes a deploy to the catalog: deployed_at is when it went live, the commit is when the catalog knew."""
        changed = self._conn.execute(
            "UPDATE cmdb.services SET version = %s, deployed_at = %s WHERE service = %s",
            (version, deployed_at, service)).rowcount
        if changed != 1:
            raise RuntimeError(f"service {service} is not in cmdb.services: python setup/01_source.py create")


class IncidentBot:
    def __init__(self, conn):
        self._conn = conn

    def open(self, service: str, severity: str, title: str, suspected_version: str | None, at: datetime) -> int:
        return self._conn.execute(
            """INSERT INTO cmdb.incidents (service, severity, status, title, suspected_version, opened_at, updated_at)
               VALUES (%s, %s, 'open', %s, %s, %s, %s) RETURNING incident_id""",
            (service, severity, title, suspected_version, at, at)).fetchone()[0]

    def set_status(self, incident_id: int, status: str, at: datetime) -> None:
        self._conn.execute("UPDATE cmdb.incidents SET status = %s, updated_at = %s WHERE incident_id = %s",
                           (status, at, incident_id))

    def set_suspect(self, incident_id: int, version: str | None, at: datetime) -> None:
        self._conn.execute("UPDATE cmdb.incidents SET suspected_version = %s, updated_at = %s WHERE incident_id = %s",
                           (version, at, incident_id))

    def delete(self, incident_id: int) -> None:
        self._conn.execute("DELETE FROM cmdb.incidents WHERE incident_id = %s", (incident_id,))
