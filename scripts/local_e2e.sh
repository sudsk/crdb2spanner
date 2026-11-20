#!/usr/bin/env bash
# End-to-end local run: movr on CockroachDB -> Spanner emulator (PG dialect).
# snapshot -> bulk load -> changefeed(cursor=T0) -> traffic -> CDC replay -> validate
#
# Prereqs: docker compose, Python 3.10+, `pip install -e ".[dev]"`
set -euo pipefail
cd "$(dirname "$0")/.."

CFG=${CFG:-examples/job.local.yaml}
TRAFFIC_SECONDS=${TRAFFIC_SECONDS:-60}
CRDB_URL="postgresql://root@localhost:26257?sslmode=disable"
c2s() { crdb2spanner "$@"; }
step() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
crdb_sql() { docker compose exec -T cockroach ./cockroach sql --url "$CRDB_URL" --format=csv -e "$1" | tail -n +2; }

step "Start CockroachDB, Spanner emulator, Pub/Sub emulator"
mkdir -p .local/extern
docker compose up -d cockroach spanner-emulator pubsub-emulator crdb-init
until [ "$(docker compose ps -a --format '{{.Service}} {{.State}}' | awk '$1=="crdb-init"{print $2}')" = "exited" ]; do sleep 2; done
docker compose logs crdb-init | tail -5

step "Create emulator instance + PostgreSQL-dialect database"
c2s dev init-emulator -c "$CFG"

step "Assess + convert"
c2s assess -c "$CFG"
c2s convert -c "$CFG"
c2s schema apply -c "$CFG" --phase tables

step "Snapshot (EXPORT AS OF SYSTEM TIME T0) + bulk load"
c2s snapshot -c "$CFG"
c2s load -c "$CFG"

step "Start changefeeds at cursor = T0"
c2s changefeed create -c "$CFG"

step "Generate ${TRAFFIC_SECONDS}s of movr write traffic"
docker compose run --rm -T movr-traffic workload run movr --duration="${TRAFFIC_SECONDS}s" --max-rate=30 \
  "postgresql://root@cockroach:26257?sslmode=disable" >/dev/null || true
STOP_HLC=$(crdb_sql "SELECT cluster_logical_timestamp()::STRING")
echo "traffic stopped at HLC $STOP_HLC"

step "Wait for changefeeds to flush past the stop point"
for _ in $(seq 1 60); do
  behind=$(crdb_sql "SELECT count(*) FROM [SHOW CHANGEFEED JOBS] WHERE status='running' AND (high_water_timestamp IS NULL OR high_water_timestamp < ${STOP_HLC})")
  [ "$behind" = "0" ] && break
  sleep 2
done
c2s changefeed list -c "$CFG"

step "Replay CDC files into Spanner (batch mode)"
c2s replicate -c "$CFG" --source files

step "Secondary indexes"
c2s schema apply -c "$CFG" --phase indexes

step "Validate (source AS OF stop point vs target)"
c2s validate -c "$CFG" --as-of "$STOP_HLC"

step "Done. Clean up with: crdb2spanner changefeed cancel -c $CFG && docker compose down -v"
