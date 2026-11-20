#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
# Phase 2 · starts Debezium Server 3.8.0.Alpha2, built from the tag, with ingest/debezium/application.properties.
#
#   conda activate zerobus-nrt-oncall    once per terminal: ingest/mask.py runs with the env's python
#   set -a; source .env; set +a          once per terminal, from the project root: exports the .env variables
#   bash ingest/debezium/run.sh          Ctrl+C stops it; the position stays in ingest/debezium/data/offsets.dat
#
# The terminal shows Debezium's console through ingest/mask.py (URLs, hosts, IDs, e-mails and the Postgres user
# masked), so it can be pasted in a chat; the full log goes to ingest/debezium/data/logs/. This script never reads
# .env, never prints a value and never copies the secret: it only checks that each variable is set.
# Optional knobs for the debezium/dbz#3026 scenarios:
#   DBZ_MAX_BATCH_SIZE (2048), DBZ_MAX_QUEUE_SIZE (8192), ZEROBUS_FLUSH_WARN_MS (30000), ZEROBUS_METRICS_LOG_INTERVAL (0),
#   ZEROBUS_SDK_LOG_LEVEL (WARN), DBZ_SNAPSHOT_MODE (initial), ZEROBUS_MAX_INFLIGHT (50000)
# After the parallel initial load (ingest/debezium/run_parallel.py), the CDC starts without a new snapshot:
#   DBZ_SNAPSHOT_MODE=no_data bash ingest/debezium/run.sh
set -eu

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
dist="$here/dist/debezium-server"
if [ ! -f "$dist/run.sh" ]; then
  echo "Debezium Server not found in ingest/debezium/dist: build it from the tag (docs, Phase 2)"
  exit 1
fi
python_bin="$(command -v python || true)"
if [ -z "$python_bin" ]; then
  echo "python not found: conda activate zerobus-nrt-oncall"
  exit 1
fi

absent=""
for name in PG_HOST PG_PORT PG_DBNAME PG_USER PG_PASSWORD UC_CATALOG ZEROBUS_SERVER_ENDPOINT \
            DATABRICKS_WORKSPACE_URL DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET; do
  eval "present=\${$name:+yes}"        # "yes" when set and not empty; the value itself is never copied
  if [ -z "$present" ]; then
    absent="$absent $name"
  fi
done
if [ -n "$absent" ]; then
  echo "missing in this shell:$absent"
  echo "from the project root, export the .env first: set -a; source .env; set +a"
  exit 1
fi

mkdir -p "$here/data/logs"
export DBZ_OFFSETS_FILE="$here/data/offsets.dat"
# The distribution carries quarkus-kubernetes-config, which builds a Kubernetes client at start even when no ConfigMap
# is used. The fabric8 client reads ~/.kube/config, and a context whose CA file is gone stops Debezium from starting.
# Debezium does not use Kubernetes here, so the client skips the kubeconfig (fabric8: kubernetes.auth.tryKubeConfig).
export KUBERNETES_AUTH_TRYKUBECONFIG=false
log="$here/data/logs/debezium-$(date +%Y%m%d-%H%M%S).log"
cp "$here/application.properties" "$dist/config/application.properties"
echo "Debezium Server 3.8.0.Alpha2: cmdb -> Zerobus -> bronze. Ctrl+C stops it."
echo "full log in ingest/debezium/data/logs/$(basename "$log"); here URLs, hosts, IDs and the Postgres user are masked"
cd "$dist"
# Ctrl+C reaches the whole pipe: tee and the mask ignore it, so Debezium shuts down cleanly and its last lines show.
# pipefail: the exit status is Debezium's, not the mask's.
set -o pipefail
status=0
bash ./run.sh 2>&1 | (trap '' INT; exec tee "$log") | (trap '' INT; exec "$python_bin" -u "$root/ingest/mask.py") \
  || status=$?
if [ -f "$DBZ_OFFSETS_FILE" ]; then
  echo "Debezium stopped (exit $status); its position is in ingest/debezium/data/offsets.dat"
else
  echo "Debezium stopped (exit $status) before saving any position"
fi
exit "$status"
