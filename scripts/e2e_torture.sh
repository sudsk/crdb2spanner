#!/usr/bin/env bash
# End-to-end "torture" test: CockroachDB incompatibilities schema -> Spanner emulator (PG dialect).
#
#   ./scripts/e2e_torture.sh            # full run (~15-25 min on an e2-standard-4)
#   SKIP_STREAMING=1 ./scripts/e2e_torture.sh
#
# Every check is recorded as PASS / FAIL and the run continues; only setup steps abort.
# Results: .local/e2e/torture-results.txt   Full log: .local/e2e/torture.log
# Prereqs: docker compose, Python 3.10+, `pip install -e ".[dev]"`, curl.
set -uo pipefail
cd "$(dirname "$0")/.."

CFG=tests/e2e/job.torture.yaml
PROBE_CFG=tests/e2e/job.torture-probes.yaml
SCEN=tests/e2e/scenarios
OUT=.local/e2e
mkdir -p "$OUT" .local/extern
exec > >(tee "$OUT/torture.log") 2>&1

RESULTS=()
FAILS=0
step()  { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
record(){ RESULTS+=("$1  $2"); [ "$1" = "FAIL" ] && FAILS=$((FAILS + 1)); printf '\033[1m[%s]\033[0m %s\n' "$1" "$2"; }
check() { local name=$1; shift; if "$@"; then record PASS "$name"; else record FAIL "$name"; fi; }
probe() { local name=$1; shift; if "$@"; then record PROBE-OK "$name"; else record PROBE-GAP "$name"; fi; }
summary() {
  step "Summary"
  printf '%s\n' "${RESULTS[@]}" | tee "$OUT/torture-results.txt"
  echo "failures: $FAILS" | tee -a "$OUT/torture-results.txt"
  echo "Send me $OUT/torture-results.txt and $OUT/torture.log"
}
must() { "$@" || { record FAIL "SETUP: $*"; summary; exit 1; }; }

c2s()       { crdb2spanner "$@"; }
crdb()      { docker compose exec -T cockroach ./cockroach sql --insecure "$@"; }
crdb_q()    { crdb -d torture --format=csv -e "$1" | tail -n +2; }
crdb_file() { crdb < "$1"; }
in_crdb()   { docker compose exec -T cockroach sh -c "$1"; }
hlc()       { crdb_q "SELECT cluster_logical_timestamp()::STRING"; }
# CockroachDB writes nodelocal files as root inside the container; make them readable by the pipelines
share_extern() { in_crdb "chmod -R a+rX /cockroach/cockroach-data/extern 2>/dev/null || true"; }
show_dlq() {  # one line per dead-lettered record: stage, table, key, error (row values omitted)
  local files; files=$(find "$1" -type f -name '*.jsonl' 2>/dev/null)
  [ -z "$files" ] && { echo "  DLQ $1: empty"; return 0; }
  echo "  DLQ $1:"
  cat $files | python3 -c '
import json, sys
for line in sys.stdin:
    try:
        r = json.loads(line)
    except ValueError:
        continue
    p = r.get("payload") or {}
    p = p if isinstance(p, dict) else {}
    print("   ", r.get("stage"), p.get("table"), p.get("key"), "->", str(r.get("error"))[:300])
' | sort | uniq -c | head -20
}
show_files() { echo "  files under $1:"; find "$1" -type f 2>/dev/null | head -5; echo "  ($(find "$1" -type f 2>/dev/null | wc -l) files)"; }
feeds_running() { crdb_q "SELECT count(*) FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running' AND $1"; }

# wait until every running changefeed whose sink matches $2 has a high-water mark past $1
wait_flush() {
  local t=$1 pat=$2 behind
  for _ in $(seq 1 90); do
    behind=$(crdb_q "SELECT count(*) FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running'
             AND description LIKE '%${pat}%' AND (high_water_timestamp IS NULL OR high_water_timestamp < ${t})")
    [ "$behind" = "0" ] && return 0
    sleep 2
  done
  echo "changefeeds did not reach $t"; return 1
}

replay()   { share_extern; c2s replicate -c "$CFG" --source files; }
validate() { c2s validate -c "$CFG" --as-of "$1"; }

# one CDC round: mark stop point, wait for the changefeeds, replay all CDC files, validate
cdc_round() {
  local name=$1 stop
  stop=$(hlc)
  check "$name: changefeeds flushed" wait_flush "$stop" "torture/cdc"
  check "$name: replicate" replay
  check "$name: validate" validate "$stop"
}

# ------------------------------------------------------------------------------------
step "0. Reset environment"
must docker compose up -d cockroach spanner-emulator pubsub-emulator crdb-init
until [ "$(docker compose ps -a --format '{{.Service}} {{.State}}' | awk '$1=="crdb-init"{print $2}')" = "exited" ]; do sleep 2; done
crdb -e "CANCEL JOBS (SELECT job_id FROM [SHOW CHANGEFEED JOBS] WHERE description LIKE '%torture%' AND status IN ('running', 'paused'))" || true
docker compose restart spanner-emulator   # the emulator is in-memory: restart = empty
until curl -sf localhost:9020/v1/projects/local-project/instanceConfigs >/dev/null; do sleep 1; done
in_crdb "rm -rf /cockroach/cockroach-data/extern/torture /cockroach/cockroach-data/extern/torture-probes"
rm -rf .local/jobs/torture .local/jobs/torture-probes .local/dlq/torture .local/dlq/torture-probes

step "1. Create torture schema + seed data"
must crdb_file tests/e2e/torture_schema.sql
must crdb_file tests/e2e/torture_seed.sql
for t in customers '"Order"' order_items events audit_log readings type_zoo '"user"' messages job_execution job_execution_params; do
  echo "  $t: $(crdb_q "SELECT count(*) FROM $t")"
done

step "2. Assess + convert + schema"
must c2s dev init-emulator -c "$CFG"
must c2s assess -c "$CFG"
check "assessment flags expected incompatibilities" python3 - <<'EOF'
import json, sys
got = {(f["table"], f["rule"]) for f in json.load(open(".local/jobs/torture/assessment.json"))}
want = {
    ("customers", "TYPE_COLLATED_STRING"), ("customers", "INVERTED_INDEX"), ("customers", "TYPE_CHAR"),
    ("Order", "COMPUTED_COLUMN"), ("Order", "PARTIAL_INDEX"), ("Order", "FOREIGN_KEYS"),
    ("events", "HIDDEN_ROWID_PK"), ("events", "TYPE_INT4"), ("events", "TYPE_INT2"), ("events", "ROW_TTL"),
    ("audit_log", "MONOTONIC_PK"), ("audit_log", "HASH_SHARDED"),
    ("readings", "HASH_SHARDED"), ("readings", "TYPE_INTERVAL"), ("readings", "TYPE_TIME"),
    ("type_zoo", "TYPE_INET"), ("type_zoo", "TYPE_BIT"),
    ("messages", "INVALID_IDENTIFIER"), ("messages", "EXPRESSION_INDEX"),
    ("job_execution", "SEQUENCE_DEFAULT"), ("job_execution", "DUPLICATE_INDEX"),
    ("job_execution", "CHECK_CONSTRAINTS"), ("job_execution_params", "HIDDEN_ROWID_PK"),
    ("messages", "NAME_COLLISION"), (None, "SEQUENCES"), (None, "SEQUENCE_INCREMENT"),
    ("customers", "DUPLICATE_INDEX"), ("user", "DUPLICATE_INDEX"),
}
missing = sorted(want - got)
# the natural key chosen for job_execution_params is unique and non-null in the seed data
unexpected = sorted(g for g in got if g[1].startswith("KEY_OVERRIDE"))
print("missing findings:", missing) if missing else print("all expected findings present")
print("unexpected findings:", unexpected) if unexpected else None
sys.exit(1 if missing or unexpected else 0)
EOF
must c2s convert -c "$CFG"
D=.local/jobs/torture
check "convert: _id renamed to id_"            grep -q 'id_ varchar NOT NULL' $D/ddl_tables.sql
check "convert: bit-reversed sequence created" grep -q 'CREATE SEQUENCE job_execution_seq BIT_REVERSED_POSITIVE' $D/ddl_tables.sql
check "convert: expression index -> generated column" grep -q 'GENERATED ALWAYS AS (lower(sender_email)) STORED' $D/ddl_tables.sql
check "convert: natural key override"          grep -q 'PRIMARY KEY (job_execution_id, key_name)' $D/ddl_tables.sql
# job_status_idx and job_status_dup_idx are identical; exactly one survives (catalog order decides which)
check "convert: duplicate index skipped"       bash -c "[ \$(grep -cE 'job_status_(dup_)?idx' $D/ddl_indexes.sql) -eq 1 ]"
check "convert: TTL policy generated"          grep -q 'ADD TTL INTERVAL' $D/ddl_post.sql
check "convert: index identical to PK skipped" bash -c "! grep -q user_id_uq $D/ddl_indexes.sql"
check "convert: index covered by unique skipped" bash -c "! grep -q customers_email_idx $D/ddl_indexes.sql"
check "convert: FK to a unique non-key column" grep -q 'REFERENCES customers (email)' $D/ddl_post.sql
must c2s schema apply -c "$CFG" --phase tables

step "3. Snapshot (EXPORT AS OF T0) + bulk load"
must c2s snapshot -c "$CFG"
share_extern
show_files .local/extern/torture/snapshot
T0=$(python3 -c "import json; print(json.load(open('.local/jobs/torture/snapshot.json'))['t0'])")
check "bulk load" c2s load -c "$CFG"
check "validate after bulk load (as of T0)" validate "$T0"
show_dlq .local/dlq/torture

step "4. Changefeeds (cursor = T0)"
must c2s changefeed create -c "$CFG"
c2s changefeed list -c "$CFG"

step "5. CDC scenarios"
for f in 01_dml_mix 02_pk_update 03_delete_reinsert; do
  check "$f: apply SQL" crdb_file "$SCEN/$f.sql"
  cdc_round "$f"
done

step "5.04 hot key: 200 sequential updates of one row"
{ echo "USE torture;"; for n in $(seq 1 200); do echo "UPDATE type_zoo SET emoji = 'v$n', big_dec = $n WHERE id = 1;"; done; } > "$OUT/04_hot_key.sql"
check "04_hot_key: apply SQL" bash -c "docker compose exec -T cockroach ./cockroach sql --insecure < '$OUT/04_hot_key.sql' > /dev/null"
cdc_round "04_hot_key"

for f in 05_big_txn 06_cascade 07_edge_values 10_real_world; do
  check "$f: apply SQL" crdb_file "$SCEN/$f.sql"
  cdc_round "$f"
done

step "6. Duplicate delivery: replay every CDC file again"
STOP=$(hlc)
check "duplicates: replicate again" replay
check "duplicates: validate" validate "$STOP"

step "7. Pause / resume changefeeds"
crdb -e "PAUSE JOBS (SELECT job_id FROM [SHOW CHANGEFEED JOBS] WHERE description LIKE '%torture/cdc%' AND status = 'running')"
check "08_during_pause: apply SQL" crdb_file "$SCEN/08_during_pause.sql"
sleep 10
crdb -e "RESUME JOBS (SELECT job_id FROM [SHOW CHANGEFEED JOBS] WHERE description LIKE '%torture/cdc%' AND status = 'paused')"
cdc_round "pause_resume"

step "8. Poison message -> DLQ, pipeline continues"
in_crdb "printf '%s\n' '{not json' '{\"after\": {\"id\": 1}, \"updated\": \"1.0\"}' > /cockroach/cockroach-data/extern/torture/cdc/type_zoo/zz-poison.ndjson"
STOP=$(hlc)
check "poison: replicate completes" replay
check "poison: parse errors in DLQ" bash -c "grep -h '\"stage\": \"parse\"' .local/dlq/torture/cdc*.jsonl | grep -q ."
check "poison: validate still green" validate "$STOP"
in_crdb "rm -f /cockroach/cockroach-data/extern/torture/cdc/type_zoo/zz-poison.ndjson"

if [ -z "${SKIP_STREAMING:-}" ]; then
  step "9. Streaming apply (--follow, strict mode) killed and restarted mid-load"
  start_follow() { setsid crdb2spanner replicate -c "$CFG" --source files --follow >"$OUT/follow_$1.log" 2>&1 & echo $!; }
  ( while true; do share_extern; sleep 3; done ) & SHARER=$!
  PID=$(start_follow 1)
  sleep 25
  check "09_streaming_load: apply SQL" crdb_file "$SCEN/09_streaming_load.sql"
  sleep 10
  kill -9 -- "-$PID" 2>/dev/null || true
  PID=$(start_follow 2)
  STOP=$(hlc)
  wait_flush "$STOP" "torture/cdc"
  ok=1
  for _ in $(seq 1 12); do
    sleep 15
    if validate "$STOP" >"$OUT/follow_validate.log" 2>&1; then ok=0; break; fi
  done
  kill -- "-$PID" 2>/dev/null || true
  kill "$SHARER" 2>/dev/null || true
  check "streaming kill/restart: converges (validate)" test "$ok" -eq 0
  if [ "$ok" -ne 0 ]; then
    echo "  last validation:"; grep -E "FAIL|different|missing|extra" "$OUT/follow_validate.log" | head -15
    for f in "$OUT"/follow_1.log "$OUT"/follow_2.log; do
      echo "  tail $f:"; grep -vE "skipping unreadable" "$f" | tail -8
    done
  fi
else
  record SKIP "streaming kill/restart"
fi

step "10. Source DDL change: the affected changefeed must stop (schema_change_policy=stop)"
crdb -d torture -e "ALTER TABLE type_zoo ADD COLUMN extra STRING DEFAULT 'x'"
sleep 30
check "schema change: type_zoo changefeed stopped" \
  test "$(feeds_running "description LIKE '%torture/cdc/type_zoo%'")" = 0
check "schema change: other changefeeds still running" \
  test "$(feeds_running "description LIKE '%torture/cdc/%' AND description NOT LIKE '%type_zoo%'")" -gt 0
c2s changefeed list -c "$CFG"

step "11. Secondary indexes + deferred FKs on the target"
check "apply secondary indexes" c2s schema apply -c "$CFG" --phase indexes
check "apply post-cutover FKs" c2s schema apply -c "$CFG" --phase post

step "12. Probes (known gaps - recorded, never fail the run)"
probe "probes: init target db" c2s dev init-emulator -c "$PROBE_CFG"
probe "probes: assess" c2s assess -c "$PROBE_CFG"
probe "probes: convert" c2s convert -c "$PROBE_CFG"
probe "probes: schema apply" c2s schema apply -c "$PROBE_CFG" --phase tables
probe "probes: assessment flags column families + geography" python3 -c "
import json; got={(f['table'], f['rule']) for f in json.load(open('.local/jobs/torture-probes/assessment.json'))}
want={('wide_families','COLUMN_FAMILIES'),('places','TYPE_GEOGRAPHY')}; print('missing:', sorted(want-got)); raise SystemExit(bool(want-got))"
probe "probes: snapshot/EXPORT (geospatial)" c2s snapshot -c "$PROBE_CFG"
share_extern
probe "probes: bulk load" c2s load -c "$PROBE_CFG"
probe "probes: changefeed create (column families)" c2s changefeed create -c "$PROBE_CFG"
crdb -d torture -e "UPDATE wide_families SET b = 'changed' WHERE id < 50; UPDATE places SET name = 'moved' WHERE id < 10" || true
PSTOP=$(hlc)
probe "probes: changefeeds flushed" wait_flush "$PSTOP" "torture-probes/cdc"
share_extern
probe "probes: replicate" c2s replicate -c "$PROBE_CFG" --source files
probe "probes: validate" c2s validate -c "$PROBE_CFG" --as-of "$PSTOP"

step "13. Clean up changefeeds"
c2s changefeed cancel -c "$CFG" || true
c2s changefeed cancel -c "$PROBE_CFG" || true

summary
[ "$FAILS" -eq 0 ]
