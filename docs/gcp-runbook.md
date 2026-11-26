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
| `gcloud` authenticated as someone who can create IAM bindings | `scripts/gcp_setup.sh` |
| Python 3.10–3.12 virtualenv on the VM | Dataflow workers run the same Python minor version as the machine that submits the job. If Dataflow rejects your Python version (3.13/3.14 may not have Dataflow worker images yet), create a 3.12 venv: `uv venv -p 3.12 .venv312 && . .venv312/bin/activate && uv pip install -e .` |
| A VPC subnet in the region for Dataflow workers | Without a `default` network, pass the VM's subnet: `--subnetwork https://www.googleapis.com/compute/v1/projects/P/regions/R/subnetworks/S` (for `e2e_gcp.sh`: `DF_EXTRA="--subnetwork ..."`). Add `--no_use_public_ips` where external IPs are not allowed; workers then need Private Google Access on the subnet and a route to PyPI (Cloud NAT) to install the package. With Shared VPC, the Dataflow service agent needs `compute.networkUser` on the host subnet |

## 1. One-off setup (idempotent)

```bash
export GCP_PROJECT=my-project REGION=europe-west2 BUCKET=my-project-c2s SPANNER_INSTANCE=c2s-test
./scripts/gcp_setup.sh
```

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

When you submit to Dataflow from a source checkout, the tool builds a wheel of the checkout and adds
`--extra_package` so the workers have the code. Passing your own `--extra_package`, `--setup_file` or
`--sdk_container_image` turns this off. For production, build the Flex Template image instead
(`templates/Dockerfile`).

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
