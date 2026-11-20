<!--
  ------------------------------------------------------------------------------
  Author:       Thomaz Antonio Rossito Neto
                Principal Data Architect & AI Strategy
                Databricks Solutions Architect Champion
  Linkedin:     in/thomaz-antonio-rossito-neto/
  Created:      2026-11-20

  -----------------------------------------------------------------------------
-->
# zerobus-nrt-oncall

Near-real-time data on Databricks, from a database commit to the on-call screen: OpenTelemetry and CDC through
Zerobus Ingest, AUTO CDC SCD1, SCD2 and bitemporal in Lakeflow, Genie, and a Databricks App that shows every change
as it arrives, with the latency of each hop.

Part 2 of [zerobus-postgres-cdc](https://github.com/ThomazRossito/zerobus-postgres-cdc).

Status: Phase 0 done (2026-11-20): environment check, Unity Catalog setup and the AUTO CDC bitemporal test.
Phase 1 done (2026-11-20): CDC source and shop simulator; two runs with seed 42 produce the same events.

## Phase 0

```bash
conda env create -f environment.yml
conda activate zerobus-nrt-oncall
cp .env.example .env
python setup/00_environment.py check
python setup/00_environment.py create
python setup/00_environment.py bitemporal
python setup/00_environment.py check
```

## Phase 1

```bash
python setup/01_source.py check
python setup/01_source.py create
python setup/01_source.py check
python sim/run.py
python sim/run.py
```

`create` sets up the local Postgres: the `cmdb` schema (`sql/phase1/cmdb.sql`), the initial service catalog, the
replication user and the `oncall_cdc` publication. `sim/run.py` plays the on-call scenario (14:00 to 14:20, one
scenario minute every 5 s by default) and prints a digest of its events; Phase 1 closes when the second run reports the
same events as the first. `python setup/01_source.py alerts` loads 10 million rows into `cmdb.alerts_history` for the
Debezium test of Phase 2.
