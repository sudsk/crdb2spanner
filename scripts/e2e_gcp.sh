#!/usr/bin/env bash
# End-to-end on real GCP: CockroachDB (docker, this VM) -> Cloud Storage -> Dataflow -> Cloud Spanner (PG).
#
#   export GCP_PROJECT=my-project REGION=europe-west2 BUCKET=my-project-c2s SPANNER_INSTANCE=c2s-test
#   ./scripts/gcp_setup.sh      # once
#   ./scripts/e2e_gcp.sh
#   ./scripts/gcp_teardown.sh   # when finished - Spanner is billed per hour
#
# Flow: seed torture DB -> assess/convert -> schema on Spanner -> EXPORT to gs:// -> Dataflow bulk load
# -> validate as of T0 -> changefeeds to gs:// -> streaming Dataflow job (bucket notifications) ->
# CDC scenarios, each validated once the stream has caught up -> drain -> indexes + FKs/CHECKs.
# Results: .local/e2e/gcp-results.txt   Log: .local/e2e/gcp.log
set -uo pipefail
cd "$(dirname "$0")/.."

: "${GCP_PROJECT:?set GCP_PROJECT}" "${BUCKET:?set BUCKET}"
REGION=${REGION:-europe-west2}
SPANNER_INSTANCE=${SPANNER_INSTANCE:-c2s-test}
WORKER_SA=${WORKER_SA:-c2s-dataflow@${GCP_PROJECT}.iam.gserviceaccount.com}
CONVERGE_TIMEOUT_S=${CONVERGE_TIMEOUT_S:-600}
export GCP_PROJECT REGION BUCKET SPANNER_INSTANCE

CFG=tests/e2e/job.torture.gcp.yaml
SCEN=tests/e2e/scenarios
OUT=.local/e2e
JD=.local/jobs/torture-gcp
SUB=c2s-torture-cdc-files
mkdir -p "$OUT"
exec > >(tee "$OUT/gcp.log") 2>&1

RESULTS=(); FAILS=0; JOB_ID=""
step()  { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
record(){ RESULTS+=("$1  $2"); [ "$1" = "FAIL" ] && FAILS=$((FAILS + 1)); printf '\033[1m[%s]\033[0m %s\n' "$1" "$2"; }
check() { local name=$1; shift; if "$@"; then record PASS "$name"; else record FAIL "$name"; fi; }
summary() {
  step "Summary"
  printf '%s\n' "${RESULTS[@]}" | tee "$OUT/gcp-results.txt"
  echo "failures: $FAILS" | tee -a "$OUT/gcp-results.txt"
  echo "Dataflow job: ${JOB_ID:-none}   Remember: ./scripts/gcp_teardown.sh"
}
must() { "$@" || { record FAIL "SETUP: $*"; [ -n "$JOB_ID" ] && g dataflow jobs cancel "$JOB_ID" --region "$REGION"; summary; exit 1; }; }

g()      { gcloud --project "$GCP_PROJECT" --quiet "$@"; }
c2s()    { crdb2spanner "$@"; }
crdb()   { docker compose exec -T cockroach ./cockroach sql --insecure "$@"; }
crdb_q() { crdb -d torture --format=csv -e "$1" | tail -n +2; }
hlc()    { crdb_q "SELECT cluster_logical_timestamp()::STRING"; }
DF=(--runner DataflowRunner --project "$GCP_PROJECT" --region "$REGION"
    --temp_location "gs://$BUCKET/dataflow/tmp" --staging_location "gs://$BUCKET/dataflow/staging"
    --service_account_email "$WORKER_SA")

wait_flush() {
  local t=$1 behind
  for _ in $(seq 1 120); do
    behind=$(crdb_q "SELECT count(*) FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running'
             AND description LIKE '%torture/cdc%' AND (high_water_timestamp IS NULL OR high_water_timestamp < ${t})")
    [ "$behind" = "0" ] && return 0
    sleep 3
  done
  echo "changefeeds did not reach $t"; return 1
}
# the streaming job applies asynchronously: re-validate until it matches or the timeout passes
converge() {
  local t=$1 start=$SECONDS
  while (( SECONDS - start < CONVERGE_TIMEOUT_S )); do
    if c2s validate -c "$CFG" --as-of "$t" > "$OUT/gcp_validate.log" 2>&1; then
      echo "  converged after $((SECONDS - start))s"; return 0
    fi
    sleep 20
  done
  grep -E "FAIL|different|missing|extra" "$OUT/gcp_validate.log" | head -15
  return 1
}
round() {  # apply a scenario, wait for the changefeeds, wait for the stream to converge
  local name=$1 file=$2 stop
  check "$name: apply SQL" bash -c "docker compose exec -T cockroach ./cockroach sql --insecure < '$file' > /dev/null"
  stop=$(hlc)
  check "$name: changefeeds flushed" wait_flush "$stop"
  check "$name: converged on Spanner" converge "$stop"
}

# ------------------------------------------------------------------------------------
step "0. Preflight + reset"
must g spanner instances describe "$SPANNER_INSTANCE" --format='value(name)'
must g pubsub subscriptions describe "$SUB" --format='value(name)'
must docker compose up -d cockroach crdb-init
until [ "$(docker compose ps -a --format '{{.Service}} {{.State}}' | awk '$1=="crdb-init"{print $2}')" = "exited" ]; do sleep 2; done
crdb -e "CANCEL JOBS (SELECT job_id FROM [SHOW CHANGEFEED JOBS] WHERE description LIKE '%torture%' AND status IN ('running','paused'))" || true
g dataflow jobs list --region "$REGION" --status active --format='value(id,name)' | awk '$2 ~ /^c2s-torture/ {print $1}' |
  while read -r j; do g dataflow jobs cancel "$j" --region "$REGION"; done
g storage rm -r "gs://$BUCKET/torture/" 2>/dev/null || true
g spanner databases delete torture --instance "$SPANNER_INSTANCE" 2>/dev/null || true
g pubsub subscriptions seek "$SUB" --time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >/dev/null   # drop old notifications
rm -rf "$JD"

step "1. Torture schema + seed data"
must bash -c "docker compose exec -T cockroach ./cockroach sql --insecure < tests/e2e/torture_schema.sql > /dev/null"
must bash -c "docker compose exec -T cockroach ./cockroach sql --insecure < tests/e2e/torture_seed.sql > /dev/null"

step "2. Assess + convert + schema on Cloud Spanner"
must c2s assess -c "$CFG"
must c2s convert -c "$CFG"
check "schema apply (creates the PostgreSQL-dialect database)" c2s schema apply -c "$CFG" --phase tables --create-database

step "3. Snapshot to Cloud Storage + Dataflow bulk load"
check "snapshot: EXPORT to gs://" c2s snapshot -c "$CFG"
g storage ls -r "gs://$BUCKET/torture/snapshot/" | grep -c parquet | xargs echo "  parquet files:"
T0=$(python3 -c "import json; print(json.load(open('$JD/snapshot.json'))['t0'])")
check "bulk load on Dataflow" c2s load -c "$CFG" -- "${DF[@]}" --job_name "c2s-torture-load-$(date +%H%M%S)"
check "validate after bulk load (as of T0)" c2s validate -c "$CFG" --as-of "$T0"

step "4. Changefeeds to gs:// (cursor = T0)"
must c2s changefeed create -c "$CFG"

step "5. Streaming Dataflow job (bucket notifications -> Spanner)"
c2s replicate -c "$CFG" --source gcs_notify -- "${DF[@]}" --job_name "c2s-torture-cdc-$(date +%H%M%S)" \
  --enable_streaming_engine --max_num_workers 2 | tee "$OUT/gcp_replicate.out"
JOB_ID=$(grep -oE 'streaming job submitted: \S+' "$OUT/gcp_replicate.out" | awk '{print $4}')
[ -n "$JOB_ID" ] || must false
for _ in $(seq 1 60); do
  state=$(g dataflow jobs describe "$JOB_ID" --region "$REGION" --format='value(currentState)')
  [ "$state" = "JOB_STATE_RUNNING" ] && break
  sleep 10
done
check "streaming job running" test "$state" = "JOB_STATE_RUNNING"
check "initial changefeed files applied" converge "$(hlc)"

step "6. CDC scenarios"
for f in 01_dml_mix 02_pk_update 03_delete_reinsert; do round "$f" "$SCEN/$f.sql"; done
{ echo "USE torture;"; for n in $(seq 1 200); do echo "UPDATE type_zoo SET emoji = 'v$n', big_dec = $n WHERE id = 1;"; done; } > "$OUT/04_hot_key.sql"
round 04_hot_key "$OUT/04_hot_key.sql"
for f in 05_big_txn 06_cascade 07_edge_values 10_real_world; do round "$f" "$SCEN/$f.sql"; done

step "7. Drain the streaming job"
g dataflow jobs drain "$JOB_ID" --region "$REGION"
g storage cat "gs://$BUCKET/torture/dlq/*" 2>/dev/null | head -5 || echo "  DLQ empty"

step "8. Secondary indexes + post-cutover FKs/CHECKs on Spanner"
check "apply secondary indexes" c2s schema apply -c "$CFG" --phase indexes
check "apply post-cutover FKs" c2s schema apply -c "$CFG" --phase post
c2s changefeed cancel -c "$CFG" || true

summary
[ "$FAILS" -eq 0 ]
