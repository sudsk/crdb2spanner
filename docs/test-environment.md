# Test environment

A self-contained local stack to develop and test `crdb2spanner` without any cloud cost:

| Service | Image | Port | Purpose |
|---|---|---|---|
| `cockroach` | `cockroachdb/cockroach:latest-v24.3` | 26257 (SQL), 8080 (console) | Source, single node, insecure |
| `crdb-init` | same | - | Enables rangefeeds, loads the **movr** sample (users, vehicles, rides, promo codes...) |
| `movr-traffic` *(profile `traffic`)* | same | - | Continuous movr writes for CDC testing |
| `spanner-emulator` | `gcr.io/cloud-spanner-emulator/emulator` | 9010 (gRPC), 9020 (REST) | Target, PostgreSQL dialect |
| `pubsub-emulator` | `google-cloud-cli:emulators` | 8085 | Exercises the Pub/Sub CDC source |

```
CockroachDB ──EXPORT (nodelocal)──► ./.local/extern/snapshot ──bulk_load──► Spanner emulator
     │                                                                        ▲
     └──changefeed (nodelocal file sink)──► ./.local/extern/cdc ──cdc_apply───┘
                                              │
                                              └─(optional) replay_to_pubsub.py ─► Pub/Sub emulator ─► cdc_apply --source pubsub
```

CockroachDB can't publish to the Pub/Sub *emulator*, so locally the changefeed writes to its
file sink (the same format as the production `gs://` sink). `scripts/replay_to_pubsub.py`
republishes those files so the Pub/Sub path can be tested too.

## 1. VM (or laptop) setup

Any Linux VM with Docker works. On GCP, `e2-standard-4` running Ubuntu 22.04/24.04 is plenty:

```bash
gcloud compute instances create c2s-dev --zone=europe-west2-b --machine-type=e2-standard-4 \
  --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud --boot-disk-size=50GB
gcloud compute ssh c2s-dev --zone=europe-west2-b

# on the VM
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2 python3-venv git
sudo usermod -aG docker $USER && newgrp docker
```

Windows works too, with Docker Desktop + WSL2: run everything inside the WSL Ubuntu shell.

## 2. Install the tool

```bash
git clone <this repo> crdbtospanner && cd crdbtospanner
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest -q            # unit + DirectRunner pipeline tests, no containers needed
```

## 3. One-command end-to-end run

```bash
./scripts/local_e2e.sh
```

What it does (each step is a normal CLI command you can run by hand):

1. `docker compose up`: CockroachDB with movr loaded, both emulators
2. `dev init-emulator`: creates the emulator instance plus a **PostgreSQL-dialect** database
3. `assess` → `.local/jobs/movr-local/assessment.md`
4. `convert` → `ddl_tables.sql`, `ddl_indexes.sql`, `ddl_post.sql`, `mapping.json`, `smt_input.sql`
5. `schema apply --phase tables`
6. `snapshot` picks T0 and runs `EXPORT ... AS OF SYSTEM TIME T0`. Then `load` (Beam DirectRunner) copies it into the emulator.
7. `changefeed create` (cursor = T0), then 60 s of movr traffic
8. `replicate --source files` replays every change into Spanner
9. `schema apply --phase indexes`
10. `validate --as-of <stop point>`: row counts and bucketed row hashes. Expect `validation passed`.

## 4. Handy commands

```bash
# SQL shell on the source
docker compose exec cockroach ./cockroach sql --insecure -d movr

# DB Console (changefeed jobs, ranges, SQL activity)
open http://localhost:8080          # or ssh -L 8080:localhost:8080 for a VM

# Bigger / different sample data
docker compose exec cockroach ./cockroach workload init tpcc --warehouses=10 'postgresql://root@localhost:26257?sslmode=disable'
docker compose exec cockroach ./cockroach workload init bank 'postgresql://root@localhost:26257?sslmode=disable'

# Continuous traffic in the background (for a following/streaming replicate)
TRAFFIC_DURATION=30m docker compose --profile traffic up -d movr-traffic
crdb2spanner replicate -c examples/job.local.yaml --source files --follow

# Query the emulator from Python
SPANNER_EMULATOR_HOST=localhost:9010 python -c "from google.cloud import spanner; \
db=spanner.Client(project='local-project').instance('local-instance').database('movr'); \
print(list(db.snapshot().execute_sql('select count(*) from rides')))"

# Exercise the Pub/Sub source
export PUBSUB_EMULATOR_HOST=localhost:8085
python scripts/replay_to_pubsub.py --config examples/job.local.yaml --project local-project
crdb2spanner replicate -c examples/job.local.yaml --source pubsub

# Reset
crdb2spanner changefeed cancel -c examples/job.local.yaml
docker compose down -v && sudo rm -rf .local
```

## 5. Licensing note (CockroachDB)

Changefeeds that write to a sink are an Enterprise feature. Recent CockroachDB releases
offer free trial and *Enterprise Free* licences. If `changefeed create` fails with a
licence error, get a key from Cockroach Labs and start the stack with it:

```bash
CRDB_ORG="EPAM" CRDB_LICENSE="crl-0-..." docker compose up -d --force-recreate crdb-init
```

## 6. Emulator caveats

- The Spanner emulator runs **one read-write transaction at a time**, so strict mode is
  slower and retries more there. Use the DirectRunner defaults locally (single process),
  and do throughput tests against a real Spanner instance.
- It doesn't model performance, hotspots or split behaviour. The emulator is only for checking that things work.
- The PG-dialect feature set of the emulator can lag behind production Spanner.

## 7. Moving to real GCP

Copy `examples/job.gcp.yaml` and fill in project, instance and bucket. Then pass Dataflow options after `--`:

```bash
python -m build --sdist
crdb2spanner load -c job.gcp.yaml -- --runner DataflowRunner --project $GCP_PROJECT \
  --region europe-west2 --temp_location gs://$BUCKET/tmp --extra_package dist/crdb2spanner-0.1.0.dev0.tar.gz
crdb2spanner replicate -c job.gcp.yaml -- --runner DataflowRunner --project $GCP_PROJECT \
  --region europe-west2 --temp_location gs://$BUCKET/tmp --extra_package dist/crdb2spanner-0.1.0.dev0.tar.gz \
  --enable_streaming_engine
```

A cheap way to get a real CockroachDB source on GCP is a single VM running the same
`cockroach` container. A free-tier CockroachDB Cloud (Basic) cluster also works.
