#!/usr/bin/env bash
# Multi-region e2e: 3-node / 3-region CockroachDB (REGIONAL BY ROW, GLOBAL, REGIONAL BY TABLE)
# -> Spanner emulator (PostgreSQL dialect): assess, convert, snapshot, bulk load, CDC incl.
# moving rows between regions, validate.
#
#   ./scripts/e2e_multiregion.sh
#
# Results: .local/e2e/multiregion-results.txt   Log: .local/e2e/multiregion.log
set -uo pipefail
cd "$(dirname "$0")/.."

CFG=tests/e2e/job.multiregion.yaml
OUT=.local/e2e
mkdir -p "$OUT" .local/extern-mr
exec > >(tee "$OUT/multiregion.log") 2>&1

RESULTS=(); FAILS=0
step()  { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
record(){ RESULTS+=("$1  $2"); [ "$1" = "FAIL" ] && FAILS=$((FAILS + 1)); printf '\033[1m[%s]\033[0m %s\n' "$1" "$2"; }
check() { local name=$1; shift; if "$@"; then record PASS "$name"; else record FAIL "$name"; fi; }
summary() {
  step "Summary"
  printf '%s\n' "${RESULTS[@]}" | tee "$OUT/multiregion-results.txt"
  echo "failures: $FAILS" | tee -a "$OUT/multiregion-results.txt"
}
must() { "$@" || { record FAIL "SETUP: $*"; summary; exit 1; }; }

c2s()    { crdb2spanner "$@"; }
crdb()   { docker compose --profile multiregion exec -T crdb-mr1 ./cockroach sql --insecure "$@"; }
crdb_q() { crdb -d mr --format=csv -e "$1" | tail -n +2; }
hlc()    { crdb_q "SELECT cluster_logical_timestamp()::STRING"; }
share()  { docker compose --profile multiregion exec -T crdb-mr1 sh -c "chmod -R a+rX /cockroach/cockroach-data/extern 2>/dev/null || true"; }
wait_flush() {
  local t=$1 behind
  for _ in $(seq 1 90); do
    behind=$(crdb_q "SELECT count(*) FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running'
             AND description LIKE '%mr/cdc%' AND (high_water_timestamp IS NULL OR high_water_timestamp < ${t})")
    [ "$behind" = "0" ] && return 0
    sleep 2
  done
  return 1
}
has_finding() { python3 -c "
import json,sys; f=json.load(open('.local/jobs/multiregion/assessment.json'))
sys.exit(0 if any(x['table']=='$1' and x['rule']=='$2' for x in f) else 1)"; }

step "0. Start 3-region CockroachDB + Spanner emulator"
must docker compose --profile multiregion up -d crdb-mr1 crdb-mr2 crdb-mr3 crdb-mr-init spanner-emulator
until [ "$(docker compose --profile multiregion ps -a --format '{{.Service}} {{.State}}' | awk '$1=="crdb-mr-init"{print $2}')" = "exited" ]; do sleep 2; done
docker compose --profile multiregion logs crdb-mr-init | tail -6
crdb -e "CANCEL JOBS (SELECT job_id FROM [SHOW CHANGEFEED JOBS] WHERE description LIKE '%mr/cdc%' AND status IN ('running','paused'))" || true
docker compose restart spanner-emulator
until curl -sf localhost:9020/v1/projects/local-project/instanceConfigs >/dev/null; do sleep 1; done
docker compose --profile multiregion exec -T crdb-mr1 rm -rf /cockroach/cockroach-data/extern/mr
rm -rf .local/jobs/multiregion .local/dlq/multiregion

step "1. Multi-region schema + data"
must crdb < tests/e2e/multiregion_schema.sql
crdb -d mr -e "SELECT crdb_region, count(*) FROM accounts GROUP BY 1 ORDER BY 1"

step "2. Assess + convert + schema"
must c2s dev init-emulator -c "$CFG"
must c2s assess -c "$CFG"
check "assess: accounts flagged REGIONAL_BY_ROW" has_finding accounts REGIONAL_BY_ROW
check "assess: fx_rates flagged GLOBAL_TABLE" has_finding fx_rates GLOBAL_TABLE
must c2s convert -c "$CFG"
grep -A6 "CREATE TABLE accounts" .local/jobs/multiregion/ddl_tables.sql
check "convert: crdb_region kept as a column" grep -q "crdb_region varchar(64)" .local/jobs/multiregion/ddl_tables.sql
check "convert: Spanner key is the logical id" grep -q "PRIMARY KEY (id)" .local/jobs/multiregion/ddl_tables.sql
check "convert: mapping records implicit region key" python3 -c "
import json; t=[t for t in json.load(open('.local/jobs/multiregion/mapping.json'))['tables'] if t['source']=='accounts'][0]
raise SystemExit(0 if t.get('source_key', [None])[0] == 'crdb_region' else 1)"
must c2s schema apply -c "$CFG" --phase tables

step "3. Snapshot + bulk load"
must c2s snapshot -c "$CFG"
share
T0=$(python3 -c "import json; print(json.load(open('.local/jobs/multiregion/snapshot.json'))['t0'])")
check "bulk load" c2s load -c "$CFG"
check "validate after bulk load" c2s validate -c "$CFG" --as-of "$T0"

step "4. CDC incl. rows moving between regions"
must c2s changefeed create -c "$CFG"
check "scenario: apply SQL" crdb < tests/e2e/multiregion_scenario.sql
STOP=$(hlc)
check "changefeeds flushed" wait_flush "$STOP"
share
check "replicate" c2s replicate -c "$CFG" --source files
check "validate after CDC" c2s validate -c "$CFG" --as-of "$STOP"
check "no DLQ entries" bash -c "! grep -qs . .local/dlq/multiregion/cdc*.jsonl"
c2s count -c "$CFG"

step "5. Secondary indexes, clean up"
check "apply secondary indexes" c2s schema apply -c "$CFG" --phase indexes
c2s changefeed cancel -c "$CFG" || true

summary
[ "$FAILS" -eq 0 ]
