# crdbtospanner

**Minimal-downtime migration from CockroachDB to Cloud Spanner (PostgreSQL dialect), built on Python and Dataflow.**

> **Status: v0.1 (pre-alpha).** The core pipeline is implemented and unit-tested on Beam DirectRunner: assess, convert, snapshot, bulk load, CDC apply and validate. The first run against live CockroachDB and the Spanner emulator is the next step (`scripts/local_e2e.sh`). Not production-ready.

## Why this exists

Google Cloud offers a guided, minimal-downtime path for MongoDB → Firestore with MongoDB compatibility (Datastream → Cloud Storage → Dataflow → Firestore). It also offers one for MySQL/PostgreSQL → Spanner, through the Spanner Migration Tool (SMT) with Datastream and Dataflow. **No equivalent exists for CockroachDB.** Datastream has no CockroachDB source, so SMT's live-migration path can't be used as-is.

`crdbtospanner` fills that gap. It works like a "DMS for CockroachDB":

- **Connection profiles** for the CockroachDB source and the Spanner target
- **Migration jobs** that move through the phases *assess → convert → load → replicate → validate → cut over*
- **Reuse of SMT** for schema conversion, and of SMT's session file as the schema-mapping contract
- **Python Apache Beam pipelines on Dataflow** for bulk load, change data capture (CDC), validation and (later) reverse replication
- **CockroachDB changefeeds** as the CDC source, replacing Datastream

## Quick start (local, no cloud cost)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest -q                      # 50 tests, no containers needed
./scripts/local_e2e.sh         # CockroachDB (movr) -> Spanner emulator, full flow + validation
```

See [docs/test-environment.md](docs/test-environment.md) for the VM setup, the sample databases and how to move to real GCP.

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
| Metadata DB, cutover gating, dashboards, Terraform | - | M2–M3 |
| Flex Templates | `templates/` | Scaffold (M3) |
| Reverse replication | - | M4 |

## How a migration works (operator view)

Modelled on Google's *Migrate to Firestore with MongoDB compatibility* guide.

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
crdb2spanner changefeed create -c $CFG          # cursor = T0, one per table
crdb2spanner replicate -c $CFG [--source pubsub|files] [--follow] [-- <beam/dataflow options>]

crdb2spanner schema apply -c $CFG --phase indexes
crdb2spanner validate -c $CFG [--as-of <HLC>]
crdb2spanner status   -c $CFG
crdb2spanner changefeed list|cancel -c $CFG
crdb2spanner schema apply -c $CFG --phase post   # FKs, after cutover
```

Profiles are sections of the job YAML (`source`, `target`). Artefacts land in `.local/jobs/<job_id>/`.

## Prerequisites

- CockroachDB **v23.1+** (to be confirmed per client) with `kv.rangefeed.enabled = true`, and changefeed licensing in place (Enterprise / Core changefeeds).
- Network egress from CockroachDB to Cloud Storage and Pub/Sub (changefeeds and `EXPORT` push data out).
- A Spanner instance with a **PostgreSQL-dialect** database.
- A GCP project with Dataflow, Pub/Sub, Cloud Storage, Secret Manager and Cloud Monitoring enabled, plus service accounts per [ARCHITECTURE.md §9](ARCHITECTURE.md#9-security-and-networking).
- Every table must have a primary key. CockroachDB's hidden `rowid` must be made explicit or replaced.

## Known limitations (v0.1)

- Single-region CockroachDB → single Spanner database per job. Multi-region mapping (`REGIONAL BY ROW`) needs per-case design.
- Foreign keys and strict interleaving are added **after** cutover, not during load and CDC.
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
├── examples/                    # job.local.yaml, job.gcp.yaml
├── scripts/                     # local_e2e.sh, replay_to_pubsub.py
├── src/crdb2spanner/
│   ├── cli/main.py              # Typer CLI
│   ├── config.py
│   ├── control/state.py         # job phases (JSON now, metadata DB in M1)
│   ├── crdb/                    # catalog introspection, changefeeds, snapshot
│   ├── assess/                  # rules + report
│   ├── schema/                  # type map, value transforms, converter, mapping, SMT bridge
│   ├── spanner/admin.py         # PG-dialect DB create, DDL apply
│   ├── pipelines/
│   │   ├── bulk_load.py         # Beam batch: Parquet -> Spanner
│   │   ├── cdc_apply.py         # Beam batch/streaming: changefeed -> Spanner
│   │   └── common/              # envelope/HLC, LWW DoFn, ops, Spanner sink, DLQ
│   └── validate/compare.py
├── templates/                   # Dataflow Flex Template scaffold
└── tests/                       # pytest + Beam DirectRunner
```

## Licensing and provenance

This project is a **clean-room implementation**. It is built from public CockroachDB, Spanner, Dataflow and Apache Beam documentation, and from Apache-2.0 Google components (SMT, DataflowTemplates, DVT, PGAdapter). It contains **no code from, and is not derived from, `storj/spanner-migration` (AGPL-3.0)**. See [PROVENANCE.md](PROVENANCE.md).

The project licence is still to be decided: EPAM proprietary, or Apache-2.0.

## Roadmap

| Milestone | Scope |
|---|---|
| M0: Spikes (2 wks) | CockroachDB → SMT schema path; `EXPORT` vs changefeed `initial_scan='only'`; Pub/Sub vs GCS sink throughput; Spanner PG-dialect writes from a Python DoFn |
| M1: Bulk + CDC MVP | Assess, convert (SMT bridge), snapshot, bulk load, CDC apply, status |
| M2: Validation and cutover | Chunked-hash validation, DVT integration, cutover gating, runbooks |
| M3: Hardening | Shadow-table strict mode, DLQ reprocessing, Terraform module, dashboards, scale tests |
| M4: Fallback | Reverse replication with Spanner change streams → CockroachDB |
