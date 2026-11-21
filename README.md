# crdbtospanner

**Minimal-downtime migration from CockroachDB to Cloud Spanner (PostgreSQL dialect), built on Python and Dataflow.**

> **Status: v0.1 (pre-alpha), in end-to-end testing.** Assess, convert, snapshot, bulk load, CDC apply and validate are implemented and unit-tested (87 tests). The full torture suite (`scripts/e2e_torture.sh`) passes against a live CockroachDB v24.3 and the Spanner emulator, with zero failures (see [Test status](#test-status)). Not yet tested on real Spanner or Dataflow. Not production-ready.

## Why this exists

Google Cloud offers a guided, minimal-downtime path for MySQL/PostgreSQL → Spanner, through the Spanner Migration Tool (SMT) with Datastream and Dataflow. **No equivalent exists for CockroachDB.** Datastream has no CockroachDB source, so SMT's live-migration path can't be used as-is.

`crdbtospanner` fills that gap. It works like a "DMS for CockroachDB":

- **Connection profiles** for the CockroachDB source and the Spanner target
- **Migration jobs** that move through the phases *assess → convert → load → replicate → validate → cut over*
- **Reuse of SMT** for schema conversion, and of SMT's session file as the schema-mapping contract
- **Python Apache Beam pipelines on Dataflow** for bulk load, change data capture (CDC), validation and (later) reverse replication
- **CockroachDB changefeeds** as the CDC source, replacing Datastream
- **No changes to the source database.** The tool reads the source as it is: no primary-key type changes, no added columns, no triggers. CockroachDB-specific constructs (hidden `rowid`, `unique_rowid()`, hash-sharded keys, `REGIONAL BY ROW`, expression indexes) are handled on the Spanner side.

## Quick start (local, no cloud cost)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest -q                      # unit + DirectRunner tests, no containers needed
./scripts/local_e2e.sh         # CockroachDB (movr) -> Spanner emulator, full flow + validation
./scripts/e2e_torture.sh       # incompatibilities schema + CDC failure scenarios (tests/e2e/README.md)
./scripts/e2e_multiregion.sh   # 3-region CockroachDB: REGIONAL BY ROW / GLOBAL / BY TABLE
```

See [docs/test-environment.md](docs/test-environment.md) for the VM setup, the sample databases and how to move to real GCP.

## Test status

`scripts/e2e_torture.sh` builds a schema of 11 tables designed to break a migration. It loads it, runs 10 CDC scenarios and validates source against target after every step (details in [tests/e2e/README.md](tests/e2e/README.md)).

| Step | State |
|---|---|
| Seed, assessment (all expected findings), convert checks, schema apply on the emulator (uuid, bit-reversed sequences, generated columns) | Passing |
| Bulk load + validation as of T0 | Passing, all 11 tables exact |
| CDC scenarios 01–10, duplicate replay, pause/resume, poison message → DLQ, schema change stops the changefeed | Passing, all 11 tables exact after every scenario |
| Secondary indexes, post-cutover FKs + CHECK constraints | Passing |
| Streaming apply (`--follow`) killed and restarted mid-load | Passing |
| Probes: column families, geospatial | Known gaps, recorded: column families need `split_column_families`; geography arrives as GeoJSON via CDC vs EWKB via EXPORT |
| Multi-region (`scripts/e2e_multiregion.sh`) | Not yet run |
| Real Spanner + Dataflow | Not yet run |

## Implementation status

| Area | Module | Status |
|---|---|---|
| Job config (YAML, env/Secret Manager) | `config.py` | Implemented |
| Catalog introspection | `crdb/catalog.py` | Implemented, needs a live-cluster check |
| Assessment report | `assess/rules.py` | Implemented (initial rule set) |
| Schema conversion → PG DDL + mapping | `schema/converter.py`, `types.py`, `values.py` | Implemented, unit-tested |
| SMT bridge (CockroachDB DDL → PG DDL for SMT) | `schema/smt_bridge.py` | Implemented. SMT import still to be checked (M0) |
| SMT session file → mapping import | - | M1 |
| Snapshot (`EXPORT ... AS OF SYSTEM TIME`) | `crdb/snapshot.py` | Implemented |
| Changefeed create/list/cancel | `crdb/changefeed.py` | Implemented |
| Bulk load (Beam) | `pipelines/bulk_load.py` | Implemented |
| CDC apply (Beam, Pub/Sub or files; LWW; strict/fast) | `pipelines/cdc_apply.py`, `pipelines/common/*` | Implemented, convergence-tested with shuffled and duplicated events |
| Validation (counts + bucketed hashes) | `validate/compare.py` | Implemented (single process) |
| Mutation-aware commits, commit delay/timeout, transaction tags | `pipelines/common/spanner_sink.py` | Implemented |
| Multi-region sources (`REGIONAL BY ROW` region kept as a column) | `schema/converter.py`, `scripts/e2e_multiregion.sh` | Implemented, needs a live run |
| Spanner client wire format: BYTES as base64 (decoded on read), `numeric` as exact text (any scale, NaN) | `schema/values.py` (`wire`, `from_wire`) | Implemented |
| Pub/Sub admin, `count`, `truncate-target` | `control/pubsub_admin.py`, `spanner/data.py` | Implemented |
| Identifier renames, database-wide names, sequences, `unique_rowid()` → sequence defaults, native `uuid`, expression / duplicate indexes, CHECK, TTL, natural-key overrides | `schema/converter.py` | Implemented, passing in e2e |
| Many databases at once (`wave`) | `control/wave.py`, `examples/wave.template.yaml` | Implemented |
| Metadata DB, cutover gating, dashboards, Terraform | - | M2–M3 |
| Flex Templates | `templates/` | Scaffold (M3) |
| Reverse replication | - | M4 |

## How a migration works (operator view)

| Stage | What happens | Source state |
|---|---|---|
| 1. Prepare | Create GCP resources (Terraform), connection profiles and network path. Enable rangefeeds on CockroachDB. | Serving |
| 2. Assess | Inventory schema, data volumes, features and blockers. Produce an assessment report. | Serving |
| 3. Convert schema | Generate PostgreSQL-dialect DDL through SMT (or the built-in converter), review the session file, apply DDL to Spanner. | Serving |
| 4. Bulk load | Export a consistent snapshot `AS OF SYSTEM TIME T0` to Cloud Storage. A Dataflow batch job loads it into Spanner. | Serving |
| 5. Replicate (CDC) | A changefeed starting at `cursor = T0` streams changes to Pub/Sub (or GCS). A Dataflow streaming job applies them idempotently. | Serving |
| 6. Validate | A Dataflow batch job compares row counts and chunked row hashes between source and target. | Serving |
| 7. Cut over | Freeze writes, wait until the resolved timestamp passes the freeze point and the backlog reaches 0, switch apps to Spanner (via PGAdapter or Spanner drivers). | Short write pause |
| 8. Fallback *(roadmap)* | Spanner change streams → Dataflow → CockroachDB, kept ready until sign-off. | Standby |

Downtime is limited to the write freeze in step 7, usually seconds to minutes, depending on replication lag at the time.

## CLI

```bash
CFG=examples/job.local.yaml          # or a copy of examples/job.gcp.yaml

crdb2spanner assess   -c $CFG        # catalog.json + assessment.md
crdb2spanner convert  -c $CFG        # ddl_tables/indexes/post.sql, mapping.json, smt_input.sql
crdb2spanner schema apply -c $CFG --phase tables [--create-database]

crdb2spanner snapshot -c $CFG        # picks T0, EXPORT ... AS OF SYSTEM TIME T0
crdb2spanner load     -c $CFG [-- <beam/dataflow options>]
crdb2spanner changefeed create -c $CFG          # cursor = T0, one per table (WITH diff where needed)
crdb2spanner replicate -c $CFG [--source pubsub|files] [--follow] [-- <beam/dataflow options>]

crdb2spanner schema apply -c $CFG --phase indexes
crdb2spanner validate -c $CFG [--as-of <HLC>]
crdb2spanner status   -c $CFG
crdb2spanner changefeed list|cancel -c $CFG
crdb2spanner schema apply -c $CFG --phase post   # FKs, CHECK, TTL - after cutover

# utilities
crdb2spanner count -c $CFG [-t TABLE] [--estimate]          # source vs target row counts
crdb2spanner truncate-target -c $CFG [-t TABLE] --yes       # empty target tables before a reload
crdb2spanner pubsub create|delete|list -c $CFG              # topics/subscriptions for the Pub/Sub sink
crdb2spanner wave --template examples/wave.template.yaml --databases-file dbs.txt --steps assess,convert
crdb2spanner dev init-emulator -c $CFG                      # local Spanner emulator instance + database
```

Profiles are sections of the job YAML (`source`, `target`). Artefacts land in `.local/jobs/<job_id>/`. A `wave` assesses and converts many databases in one go and writes a summary to `.local/waves/<name>/summary.md`.

**Runners.** Anything after `--` goes to Beam. With no `--runner`, local batch runs (`load`, `replicate`) use Beam's FnApiRunner (`C2S_LOCAL_RUNNER=PrismRunner` overrides). Locally, `replicate --source files --follow` polls for new changefeed files every few seconds and applies each new set with a short batch pipeline, because Beam's local streaming runners are unreliable with continuously-matched files. Restarting it is safe: strict mode keeps last-writer-wins. On Dataflow, `--follow` is a real streaming pipeline. On GCP, pass `--runner DataflowRunner --project P --region R --temp_location gs://...`.

## Prerequisites

- CockroachDB **v23.1+** (to be confirmed per client) with `kv.rangefeed.enabled = true`, and changefeed licensing in place (Enterprise / Core changefeeds).
- Network egress from CockroachDB to Cloud Storage and Pub/Sub (changefeeds and `EXPORT` push data out).
- A Spanner instance with a **PostgreSQL-dialect** database.
- A GCP project with Dataflow, Pub/Sub, Cloud Storage, Secret Manager and Cloud Monitoring enabled, plus service accounts per [ARCHITECTURE.md §9](ARCHITECTURE.md#9-security-and-networking).
- No source schema changes are required. Tables without a primary key (hidden `rowid`) are migrated with a bit-reversed key on Spanner, or with a natural key set in `primary_key_overrides`. Tables that use `diff`-dependent features (natural-key overrides) need their changefeed created `WITH diff`, which `changefeed create` does automatically.

## Known limitations (v0.1)

- One CockroachDB database → one Spanner database per job (use `wave` for many). `REGIONAL BY ROW` keeps the region as an ordinary column; Spanner placement policy is a per-case design decision.
- Foreign keys, CHECK constraints and TTL policies are added **after** cutover (`--phase post`), not during load and CDC. Strict interleaving isn't generated.
- Column families have no Spanner equivalent and are flattened; very wide tables with large families are flagged by `assess`.
- Spanner can't index `jsonb`, `numeric`, `real` or array columns. Such indexes are skipped and reported by `convert`.
- Schema changes on the source during replication aren't propagated automatically. They need a pause-and-apply procedure.
- Reverse replication (fallback) is on the roadmap, not in v1.

## Repository layout

```
crdbtospanner/
├── README.md · ARCHITECTURE.md · PROVENANCE.md
├── docker-compose.yml           # CockroachDB + movr, Spanner emulator, Pub/Sub emulator
├── docs/
│   ├── test-environment.md
│   └── type-mapping.md
├── examples/                    # job.local.yaml, job.gcp.yaml, wave.template.yaml
├── scripts/                     # local_e2e.sh, e2e_torture.sh, e2e_multiregion.sh, replay_to_pubsub.py
├── src/crdb2spanner/
│   ├── cli/main.py              # Typer CLI
│   ├── config.py
│   ├── control/                 # job state, Pub/Sub admin, waves
│   ├── crdb/                    # catalog introspection, changefeeds, snapshot
│   ├── assess/                  # rules + report
│   ├── schema/                  # type map, value transforms, converter, mapping, SMT bridge
│   ├── spanner/                 # PG-dialect DB create, DDL apply, counts, truncate
│   ├── pipelines/
│   │   ├── bulk_load.py         # Beam batch: Parquet -> Spanner
│   │   ├── cdc_apply.py         # Beam batch/streaming: changefeed -> Spanner
│   │   └── common/              # envelope/HLC, LWW DoFn, ops, Spanner sink, DLQ
│   └── validate/compare.py
├── templates/                   # Dataflow Flex Template scaffold
└── tests/                       # pytest (unit + Beam) and e2e/ (torture + multi-region suites)
```

## Licensing and provenance

Licensed under the [Apache License 2.0](LICENSE). See also [NOTICE](NOTICE).

This project is built from public CockroachDB, Spanner, Dataflow and Apache Beam documentation, and from Apache-2.0 Google components (SMT, DataflowTemplates, DVT, PGAdapter). See [PROVENANCE.md](PROVENANCE.md).

## Why Python

The work is I/O-bound: CockroachDB export and changefeed emission, reading Cloud Storage files and Spanner commit latency. Throughput comes from parallel commits across Dataflow workers and shards, and from Spanner node count, not from per-row CPU. The Beam Python SDK also has the more mature Dataflow features (Flex Templates, streaming, stateful processing). If a table turns out to be CPU-bound, raise `num_shards` and the worker count, or vectorise the hot transform with pyarrow.

## Roadmap

| Milestone | Scope |
|---|---|
| M0: Spikes (2 wks) | CockroachDB → SMT schema path; `EXPORT` vs changefeed `initial_scan='only'`; Pub/Sub vs GCS sink throughput; Spanner PG-dialect writes from a Python DoFn |
| M1: Bulk + CDC MVP | Assess, convert (SMT bridge), snapshot, bulk load, CDC apply, status |
| M2: Validation and cutover | Chunked-hash validation, DVT integration, cutover gating, runbooks, `gcs_notify` mode (Cloud Storage notifications → Pub/Sub instead of polling) |
| M3: Hardening | Shadow-table strict mode, DLQ reprocessing, Terraform module, dashboards, scale tests |
| M4: Fallback | Reverse replication with Spanner change streams → CockroachDB |
