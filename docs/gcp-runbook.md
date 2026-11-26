# Running on GCP: Cloud Storage + Dataflow + Cloud Spanner

This runbook takes the torture test database from the CockroachDB on the test VM to a real Cloud Spanner
(PostgreSQL dialect) instance. Dataflow does both the bulk load and the streaming CDC apply. The same
steps apply to a real source; only the job YAML changes.

```
CockroachDB ──EXPORT AS OF T0──► gs://BUCKET/torture/snapshot ──Dataflow (batch)──────► Spanner
            ──changefeeds──────► gs://BUCKET/torture/cdc ──bucket notification──► Pub/Sub
                                                          ──Dataflow (streaming, reads each new file)──► Spanner
```

There is no Datastream source for CockroachDB, so changefeeds write newline-delimited JSON files to
Cloud Storage. Cloud Storage announces each new file on a Pub/Sub subscription, and the streaming
Dataflow job (`replicate --source gcs_notify`) reads exactly those files. Nothing lists or polls the
bucket.

## 0. Prerequisites

| What | Why |
|---|---|
| The test VM with the docker compose stack (`docker compose up -d cockroach`) | Source CockroachDB |
| VM access scope **cloud-platform** | CockroachDB writes to Cloud Storage with the VM identity (`AUTH=implicit`). The default scopes only allow reading from Cloud Storage. Stop the VM, then `gcloud compute instances set-service-account VM --zone Z --scopes cloud-platform` |
| `gcloud` authenticated as someone who can create IAM bindings | `scripts/gcp_setup.sh`. Easiest from Cloud Shell as yourself (see 1) |
| Python 3.10–3.12 virtualenv on the VM | Dataflow workers run the same Python minor version as the machine that submits the job. If Dataflow rejects your Python version (3.13/3.14 may not have Dataflow worker images yet), create a 3.12 venv: `uv venv -p 3.12 .venv312 && . .venv312/bin/activate && uv pip install -e .` |
| A VPC subnet in the region for Dataflow workers | See [Networking for Dataflow workers](#networking-for-dataflow-workers) |

## 1. One-off setup (idempotent)

```bash
export GCP_PROJECT=my-project REGION=europe-west2 BUCKET=my-project-c2s SPANNER_INSTANCE=c2s-test
./scripts/gcp_setup.sh                       # on the VM, if its service account may change IAM (rare)
VM_SA=<n>-compute@developer.gserviceaccount.com ./scripts/gcp_setup.sh   # from Cloud Shell, as yourself
```

Running it from Cloud Shell is the usual case: the VM's default service account can't create IAM
bindings. `VM_SA` names the VM's service account so the script grants it the roles below; the VM's
access scopes still have to be fixed on the VM itself (Prerequisites).

This creates:
- the bucket
- the Spanner instance: 100 processing units, regional, around $0.10–0.15 an hour; delete it afterwards
- the Pub/Sub topic and subscription `c2s-torture-cdc-files`, plus a bucket notification for `torture/cdc/`
- the Dataflow worker service account `c2s-dataflow`, with roles `dataflow.worker`, `spanner.databaseUser`, `pubsub.subscriber` and object admin on the bucket
- roles for the VM's service account: `spanner.databaseAdmin`, `dataflow.developer`, object admin on the bucket, and permission to run jobs as the worker service account

**Free trial instance.** A Spanner free trial instance (90 days, 10 GB, a tenth of a node) is enough
for this functional test. It supports PostgreSQL-dialect databases and regional configurations. Create
it in the console and set `SPANNER_INSTANCE` to its ID; `gcp_setup.sh` then leaves it alone. It is not
meant for performance testing, and a project gets only one, so tear down with `KEEP_SPANNER=1`.

## 2. Run the whole thing

```bash
./scripts/e2e_gcp.sh          # ~30-45 min; results in .local/e2e/gcp-results.txt
./scripts/gcp_teardown.sh     # stop paying for Spanner
```

## 3. Or step by step

```bash
CFG=tests/e2e/job.torture.gcp.yaml
WORKER_SA=c2s-dataflow@$GCP_PROJECT.iam.gserviceaccount.com
DF="--runner DataflowRunner --project $GCP_PROJECT --region $REGION \
    --temp_location gs://$BUCKET/dataflow/tmp --staging_location gs://$BUCKET/dataflow/staging \
    --service_account_email $WORKER_SA"

crdb2spanner assess  -c $CFG
crdb2spanner convert -c $CFG
crdb2spanner schema apply -c $CFG --phase tables --create-database

crdb2spanner snapshot -c $CFG                         # EXPORT ... AS OF T0 -> gs://BUCKET/torture/snapshot
crdb2spanner load -c $CFG -- $DF --job_name c2s-torture-load
crdb2spanner validate -c $CFG --as-of "$(jq -r .t0 .local/jobs/torture-gcp/snapshot.json)"

crdb2spanner changefeed create -c $CFG                # cursor = T0, files -> gs://BUCKET/torture/cdc
crdb2spanner replicate -c $CFG --source gcs_notify -- $DF --job_name c2s-torture-cdc \
    --enable_streaming_engine --max_num_workers 2     # returns once the streaming job is submitted
crdb2spanner changefeed list -c $CFG                  # lag per changefeed
crdb2spanner validate -c $CFG                         # source AS OF now-ish vs Spanner

# cutover: stop writes on the source, wait for changefeed lag ~0 and a green validate, then
gcloud dataflow jobs drain JOB_ID --region $REGION
crdb2spanner schema apply -c $CFG --phase indexes
crdb2spanner schema apply -c $CFG --phase post        # FKs, CHECK constraints, TTL
crdb2spanner changefeed cancel -c $CFG
```

How the workers get the code: with `C2S_WORKER_IMAGE` set, they run on that pre-built image
(see [Workers without PyPI access](#workers-without-pypi-access-pre-built-image)). Otherwise, when you
submit from a source checkout, the tool builds a wheel of the checkout and adds `--extra_package`, and
the workers install its dependencies from PyPI at startup. Passing your own `--extra_package`,
`--setup_file` or `--sdk_container_image` turns both off.

## Workers without PyPI access: pre-built image

In projects where Dataflow workers have no external IP and no Cloud NAT, workers can't install
packages at startup. Build an image that already contains the tool and every dependency, once per
tool version:

```bash
export GCP_PROJECT=my-project REGION=europe-west2
./scripts/build_worker_image.sh          # Cloud Build -> Artifact Registry repo "c2s"; LOCAL_DOCKER=1 to use docker
export C2S_WORKER_IMAGE=<printed image URI>
crdb2spanner load -c $CFG -- $DF --subnetwork <url> --no_use_public_ips --job_name c2s-load
```

- The image is built for the Python and Apache Beam versions of the environment that runs the
  script; submit the jobs from that same environment (Dataflow requires matching versions).
- With `C2S_WORKER_IMAGE` set, the tool adds `--sdk_container_image <image> --sdk_location container`
  to every Dataflow job; nothing is staged for the workers to install.
- The worker service account needs `roles/artifactregistry.reader` (`gcp_setup.sh` grants it).
- Building needs the Cloud Build and Artifact Registry APIs (the script enables them). Organisations
  that restrict Cloud Build's default service account may need `gcloud builds submit
  --service-account ...`, or `LOCAL_DOCKER=1` from a machine with docker.

## Source CockroachDB user (least privilege)

The tool only reads the source. It never creates tables, temp tables or schema objects there, so
the migration user needs no `CREATE` privilege:

```sql
CREATE USER migrator WITH PASSWORD '...';                        -- or certificate auth
GRANT CONNECT ON DATABASE app_db TO migrator;
USE app_db;
GRANT USAGE ON SCHEMA public TO migrator;
GRANT SELECT ON TABLE public.* TO migrator;                      -- assess, EXPORT, validate
GRANT SELECT ON SEQUENCE public.* TO migrator;                   -- current values for SKIP RANGE
GRANT CHANGEFEED ON TABLE public.* TO migrator;                  -- CREATE CHANGEFEED
GRANT SYSTEM EXTERNALIOIMPLICITACCESS TO migrator;               -- gs://...?AUTH=implicit (EXPORT, changefeeds)
GRANT SYSTEM VIEWCLUSTERSETTING TO migrator;                     -- optional: lets assess read kv.rangefeed.enabled
```

An admin also has to enable rangefeeds once: `SET CLUSTER SETTING kv.rangefeed.enabled = true;`.
With `AUTH=specified` (a service account key in `cdc.sink_credentials_b64`), `EXTERNALIOIMPLICITACCESS`
is not needed. The user can cancel its own changefeeds; `CONTROLJOB` is only needed to manage
changefeeds created by someone else.

## Network paths

Data flows from CockroachDB to Cloud Storage and from Cloud Storage to Spanner. Dataflow never
connects to CockroachDB, so there is no inbound path from Google Cloud to the source database.

| From | To | Port | Why |
|---|---|---|---|
| CockroachDB nodes | `storage.googleapis.com` | 443 | `EXPORT` snapshot files and changefeed files |
| Operator machine (VM, Cloud Shell, CI) | CockroachDB SQL endpoint | 26257 | `assess`, `snapshot`, `changefeed create`, `validate` |
| Operator machine | Google APIs (Spanner, Dataflow, Storage) | 443 | schema apply, job submission, `validate` |
| Dataflow workers | Google APIs (Storage, Pub/Sub, Spanner, Artifact Registry) | 443 | through Private Google Access when workers have no external IP |
| Dataflow workers | Dataflow workers | 12345-12346 | worker-to-worker traffic (needed by every Dataflow job) |

## Spanner roles

| Principal | Role | Why |
|---|---|---|
| Operator | `roles/spanner.databaseAdmin` on the instance | creates the database, applies the DDL phases, runs `validate` |
| Dataflow worker service account | `roles/spanner.databaseUser` on the instance (default), or the DML-only custom role `c2sSpannerDml` (`WORKER_SPANNER_ROLE=dml ./scripts/gcp_setup.sh`) | reads and writes rows, including the strict-mode shadow tables; never changes the schema. `databaseUser` also allows DDL, which the custom role leaves out |

## Networking for Dataflow workers

Dataflow starts its workers on the network called `default` unless told otherwise. Pick the case that
matches the project:

| Project network | Dataflow options to add | Also needed |
|---|---|---|
| `default` VPC exists, external IPs allowed | none | - |
| No `default` VPC (common in organisation projects), external IPs allowed | `--subnetwork regions/<region>/subnetworks/<subnet>` (the VM's subnet, in the job's region; the full URL also works and is required for Shared VPC) | - |
| External IPs not allowed (org policy `compute.vmExternalIpAccess`) | `--subnetwork <url> --no_use_public_ips` | Private Google Access on the subnet (workers reach Cloud Storage, Pub/Sub, Spanner), and either Cloud NAT so workers can `pip install` the package and its dependencies from PyPI, **or** a [pre-built worker image](#workers-without-pypi-access-pre-built-image) (`scripts/build_worker_image.sh`, then `C2S_WORKER_IMAGE`; no PyPI needed) |
| Shared VPC (the subnet URL names another project) | `--subnetwork <url in the host project>` | The Dataflow service agent `service-<project number>@dataflow-service-producer-prod.iam.gserviceaccount.com` and the worker service account need `roles/compute.networkUser` on that subnet |

Find the VM's subnet, and whether it has an external IP (Cloud Shell):

```bash
gcloud compute instances describe VM --zone Z \
  --format='value(networkInterfaces[0].subnetwork,networkInterfaces[0].accessConfigs[0].natIP)'
```

For `e2e_gcp.sh`, pass the options in `DF_EXTRA`, e.g. `DF_EXTRA="--subnetwork regions/europe-west2/subnetworks/S"`.
Step by step, add them after `$DF`: `crdb2spanner load -c $CFG -- $DF --subnetwork regions/europe-west2/subnetworks/S --job_name c2s-torture-load`.

## Troubleshooting (seen on real projects)

| Symptom | Cause | Fix |
|---|---|---|
| `ACCESS_TOKEN_SCOPE_INSUFFICIENT` from `gcloud` or the tool on the VM | VM created with default access scopes | Stop the VM; Edit → API and identity management → **Allow full access to all Cloud APIs** (= `cloud-platform`); start it. IAM roles then decide access |
| `Service account c2s-dataflow@… does not exist` right after it was created | IAM propagation delay | `gcp_setup.sh` retries for up to 2 minutes; otherwise re-run it (idempotent) |
| `PERMISSION_DENIED … pubsub.subscriptions.consume` on `subscriptions seek` | VM service account had only viewer access to the subscription | Re-run `gcp_setup.sh` (now grants subscriber on that subscription), or skip the seek on a first run |
| CockroachDB `EXPORT` / changefeed: permission or credential errors writing `gs://` | VM scopes, or no bucket role for the VM service account | Fix scopes (above); re-run `gcp_setup.sh` with `VM_SA` |
| Dataflow: `Network default is not accessible to Dataflow Service account or does not exist` | Project has no `default` VPC | `--subnetwork <url>` ([Networking](#networking-for-dataflow-workers)) |
| Dataflow workers stuck installing packages, or `pip` timeouts in worker logs | Workers without external IPs and no Cloud NAT | Cloud NAT on the subnet, or a [pre-built worker image](#workers-without-pypi-access-pre-built-image) |
| Dataflow rejects the job's Python version | No worker image for the submitting Python version | Submit from a Python 3.12 venv (Prerequisites) |
| Warning: temp bucket has soft-delete enabled | Cost notice only | Optionally turn off soft delete on the bucket |
| Warning: `.whl … must be binary-compatible with the worker runtime` | The package is pure Python | Ignore |

## Notes

- **Notifications only cover new files.** Create the bucket notification before the changefeeds, as
  `gcp_setup.sh` does. To replay files that already exist (a backfill, or recovery after deleting the
  subscription), run a batch `replicate --source files -- $DF`. It's safe to combine with the
  streaming job because strict mode keeps last-writer-wins.
- **Ordering.** Files can be read in any order and any number of times: changes are applied
  last-writer-wins on the CockroachDB HLC timestamp, and strict mode records the winning HLC per row in
  the shadow tables.
- **Credentials without the VM identity.** Set `cdc.sink_credentials_b64` (a base64 service account
  key) to use `AUTH=specified` instead of `AUTH=implicit`. Many organisations block service account
  keys; prefer workload identity.
- **Real sources.** Change `source.dsn` (or `dsn_secret`), the bucket paths and the Spanner database,
  and keep `apply.mode: strict` for CDC. Size `num_shards` (parallel commit streams) against the Spanner
  instance; measure it on a representative database before the real waves.
