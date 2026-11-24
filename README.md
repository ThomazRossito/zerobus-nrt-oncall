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

Status: Phase 0 done (2026-11-20): environment check and Unity Catalog setup.
Phase 1 done (2026-11-20): CDC source and shop simulator; two runs with seed 42 produce the same events.
Phase 2 done (2026-11-20): CDC and OpenTelemetry through Zerobus Ingest; commit → bronze latency p50 5.4 s, p95 8.2 s;
the test plan of [debezium/dbz#3026](https://github.com/debezium/dbz/issues/3026) on Debezium Server 3.8.0.Alpha2.
Phase 3 done (2026-11-22): the Lakeflow pipeline in the bundle, SQL on serverless; at 14:05 the as-of queries answer
2.4.1 for the version that was live and 2.4.0 for what the catalog said, and the deleted incident leaves SCD1.
Phase 4 done (2026-11-24): a Genie Agent in the bundle over the Phase 3 tables; Genie One sends the postmortem
question to it and answers 2.4.1 for the version that was live and 2.4.0 for what the catalog said, in English and
in Portuguese. Extended the same day with the OpenTelemetry logs and spans and with the table of the bitemporal post
on the scenario clock, which Genie One returns through the agent when the question names the incident.

## Phase 0

```bash
conda env create -f environment.yml
conda activate zerobus-nrt-oncall
cp .env.example .env
python setup/00_environment.py check
python setup/00_environment.py create
python setup/00_environment.py check
```

Until 2026-11-24, Phase 0 also deployed a two-row AUTO CDC bitemporal test pipeline. It left the bundle because the
Phase 3 pipeline answers the same two questions with the project's data. If you ran it, run
`databricks bundle deploy --var catalog=<your catalog>` once from `bundle/` and confirm if the CLI asks: it deletes the
`zerobus_nrt_oncall_phase0_bitemporal` pipeline. A deleted pipeline keeps its tables, so then drop
`bronze.phase0_bitemporal_source` and `silver.phase0_bitemporal`.

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

## Phase 2: CDC and OpenTelemetry through Zerobus

CDC: bronze tables, then the 10 million row initial load in parallel and the CDC from a new slot.

```bash
python setup/02_cdc.py create            # bronze tables and Zerobus grants
python setup/02_cdc.py reset --yes       # no slot, no offsets, empty bronze before a new load
conda activate zerobus-nrt-oncall        # in a second terminal
set -a; source .env; set +a
python ingest/debezium/run_parallel.py   # 8 Debezium Server instances, one alert_id range each
python setup/02_cdc.py check             # one snapshot row per alert_id
DBZ_SNAPSHOT_MODE=no_data bash ingest/debezium/run.sh   # the CDC, without a new snapshot
python measure/latency.py                # commit → bronze latency, part 1's ruler
```

OpenTelemetry: the three OTel tables, the Collector and one run of the sim over OTLP.

```bash
python setup/02_otel.py create           # otel.shop_otel_spans, _logs, _metrics and their grants
conda activate zerobus-nrt-oncall        # in a third terminal
set -a; source .env; set +a
bash ingest/otel/run.sh                  # otelcol-contrib, OTLP on 127.0.0.1:4317 to Zerobus
python sim/run.py --otlp                 # the scenario, with its telemetry over OTLP
python setup/02_otel.py check            # the run's spans and logs, all in the tables
```

`ingest/debezium/application.properties` and `ingest/otel/collector.yaml` read every value from the environment,
the OAuth secret included; no Python script reads it. A `RegexRouter` sends `<catalog>.cmdb.<table>` to
`<catalog>.bronze.<table>`; `timestamptz` columns land as STRING and silver converts them. The Collector follows the
example of the Databricks docs: one `oauth2client` per table, which renews the token before it expires.

The terminals show Debezium's and the Collector's consoles through `ingest/mask.py`, with URLs, hosts, IDs, e-mails
and the Postgres user masked, so they can be pasted in an issue or a chat; the full logs stay in
`ingest/debezium/data/logs/` and `ingest/otel/data/logs/`, outside git.

## Phase 3: the pipeline

The Lakeflow pipeline in the bundle, in SQL on serverless (`bundle/resources/oncall.pipeline.yml` and
`bundle/src/pipeline/`): every change in silver (`services_changes`, `incidents_changes`), AUTO CDC SCD1
(`incidents_current`), SCD2 (`services_history`) and bitemporal (`services_bitemporal`), and gold (`service_health`,
`errors_by_version`, `incident_postmortem`, `hop_latency`). It reads what Phase 2 left in bronze and in the OTel tables;
Debezium and the Collector do not need to be running.

```bash
python setup/03_pipeline.py check        # read-only: the inputs, and the results once the pipeline ran
python setup/03_pipeline.py run          # deploys the bundle, runs one update and checks the results
```

Phase 3 closes when the as-of queries answer 2.4.1 for the version that was live when the incident opened and 2.4.0
for what the catalog said at that moment, and the deleted incident is not in `silver.incidents_current`.
`sql/phase3/bitemporal_in_action.sql` shows it in the SQL editor: the two answers from the same table, where only the
system-time filter changes.

## Phase 4: Genie

A Genie Agent in the bundle (`bundle/resources/oncall.genie.yml`, the `genie_spaces` resource, Databricks CLI 1.3.0 or
later with the direct deployment engine): five Phase 3 tables (`gold.incident_postmortem`, `gold.errors_by_version`,
`gold.service_health`, `gold.hop_latency`, `silver.services_bitemporal`) and two OpenTelemetry tables
(`otel.shop_otel_logs`, `otel.shop_otel_spans`) with column descriptions, eight sample questions with example SQL
and one block of instructions. `check` runs every example SQL on the warehouse before a deploy. Its warehouse is
`WAREHOUSE_ID` of `.env`, a pro or serverless one. The scenario clock example returns the six columns of the bitemporal
post and `first_known`, when the catalog first named each version.

```bash
python setup/04_genie.py check           # read-only: tables, the expected answer, warehouse, CLI, the agent
python setup/04_genie.py run             # deploys the bundle and asks the agent the postmortem question
python setup/04_genie.py agent           # asks the agent directly, without Genie One in between
python setup/04_genie.py ask             # asks Genie One the same question (databricks genie ask)
python setup/04_genie.py ask "QUESTION"  # another question for Genie One, in quotes
python setup/04_genie.py chats           # opens the Genie One chats of the saved answers, to delete them there
```

Phase 4 closes when Genie One answers the postmortem question correctly: 2.4.1 was live when the latest
"Checkout error rate above 5%" incident opened, and the catalog named 2.4.0 at that moment. The question names the
incident instead of 14:05 because the tables keep the real time of the run, in UTC.

Genie One first looks for a Genie Agent relevant to the question, and it draws on your past conversations. `ask` says
whether it handed the question to this agent (`ask_genie_space`) and whether it read an earlier chat (`episode_fetch`).
For a test from scratch, delete the earlier chats first: `chats` opens in the browser the chats of the saved answers
that it has not opened before (`--all` opens them all again), and the rest are in the Genie One sidebar. When Genie One
did not use this agent, `ask` prints only the first lines of the answer, with other catalogs masked, since it is about
other assets of the workspace; the `.local` file keeps it all.
