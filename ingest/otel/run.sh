#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# Author:       Thomaz Antonio Rossito Neto
#               Principal Data Architect & AI Strategy
#               Databricks Solutions Architect Champion
# Linkedin:     in/thomaz-antonio-rossito-neto/
# Created:      2026-11-20
#
# -----------------------------------------------------------------------------
# Phase 2 · starts the OTel Collector (otelcol-contrib) with ingest/otel/collector.yaml: OTLP from the sim on
# 127.0.0.1:4317, to Zerobus Ingest, into <catalog>.otel.shop_otel_spans, shop_otel_logs and shop_otel_metrics.
#
#   conda activate zerobus-nrt-oncall    once per terminal: ingest/mask.py runs with the env's python
#   set -a; source .env; set +a          once per terminal, from the project root: exports the .env variables
#   bash ingest/otel/run.sh              Ctrl+C stops it
#
# The terminal shows the Collector's console through ingest/mask.py (URLs, hosts, IDs and e-mails masked), so it can be
# pasted in a chat; the full log goes to ingest/otel/data/logs/. This script never reads .env and never prints a value:
# it checks that each variable is set and derives, without printing them, the Zerobus host, the workspace ID and the
# token URL the Collector needs. The Collector gets the secret from this shell, as Debezium does.
# Optional knob: OTEL_BATCH_TIMEOUT (5s, as in the docs example), the timeout of the batch processor.
set -eu

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
collector="$(command -v otelcol-contrib || true)"
if [ -z "$collector" ]; then
  echo "otelcol-contrib not found: install the darwin_arm64 release (docs, Phase 0)"
  exit 1
fi
python_bin="$(command -v python || true)"
if [ -z "$python_bin" ]; then
  echo "python not found: conda activate zerobus-nrt-oncall"
  exit 1
fi

absent=""
for name in UC_CATALOG ZEROBUS_SERVER_ENDPOINT DATABRICKS_WORKSPACE_URL DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET; do
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
if (exec 3<>/dev/tcp/127.0.0.1/4317) 2>/dev/null; then
  echo "127.0.0.1:4317 is taken: another Collector is running (stop it with Ctrl+C in its terminal)"
  exit 1
fi

# The Zerobus host without scheme, path or port (<workspace-id>.zerobus.<region>.azuredatabricks.net, checked in
# Phase 0), its first label as the workspace ID, and the workspace's OAuth token URL, as in the docs example.
host="${ZEROBUS_SERVER_ENDPOINT#*://}"
host="${host%%/*}"
host="${host%%:*}"
url="${DATABRICKS_WORKSPACE_URL%/}"
case "$url" in
  http://*|https://*) ;;
  *) url="https://$url" ;;
esac
export ZNO_OTLP_ENDPOINT="$host:443"
export ZNO_WORKSPACE_ID="${host%%.*}"
export ZNO_TOKEN_URL="$url/oidc/v1/token"
export OTEL_BATCH_TIMEOUT="${OTEL_BATCH_TIMEOUT:-5s}"

mkdir -p "$here/data/logs"
log="$here/data/logs/collector-$(date +%Y%m%d-%H%M%S).log"
echo "$("$collector" --version): OTLP on 127.0.0.1:4317 -> Zerobus -> $UC_CATALOG.otel.shop_otel_{spans,logs,metrics}"
echo "batch timeout $OTEL_BATCH_TIMEOUT. Ctrl+C stops it."
echo "full log in ingest/otel/data/logs/$(basename "$log"); here URLs, hosts, IDs and e-mails are masked"
cd "$root"
# Ctrl+C reaches the whole pipe: tee and the mask ignore it, so the Collector shuts down cleanly and its last lines
# show. pipefail: the exit status is the Collector's, not the mask's.
set -o pipefail
status=0
"$collector" --config "$here/collector.yaml" 2>&1 | (trap '' INT; exec tee "$log") \
  | (trap '' INT; exec "$python_bin" -u "$root/ingest/mask.py") || status=$?
echo "OTel Collector stopped (exit $status)"
exit "$status"
