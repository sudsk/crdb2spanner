#!/usr/bin/env bash
# One-off GCP setup for the crdb2spanner GCP test (idempotent: safe to re-run).
#
#   export GCP_PROJECT=my-project REGION=europe-west2 BUCKET=my-project-c2s SPANNER_INSTANCE=c2s-test
#   ./scripts/gcp_setup.sh
#   VM_SA=<n>-compute@developer.gserviceaccount.com ./scripts/gcp_setup.sh   # from Cloud Shell / laptop
#
# Creates: APIs, bucket, Spanner instance (100 processing units, regional), Pub/Sub topic + subscription
# fed by bucket notifications for torture/cdc/, a Dataflow worker service account and IAM bindings for it
# and for this VM's service account. Costs money while it exists: run scripts/gcp_teardown.sh afterwards.
set -euo pipefail

: "${GCP_PROJECT:?set GCP_PROJECT}"
REGION=${REGION:-europe-west2}
BUCKET=${BUCKET:-${GCP_PROJECT}-c2s}
SPANNER_INSTANCE=${SPANNER_INSTANCE:-c2s-test}
SPANNER_UNITS=${SPANNER_UNITS:-100}
TOPIC=c2s-torture-cdc-files
SUB=c2s-torture-cdc-files
WORKER_SA_NAME=c2s-dataflow
WORKER_SA=${WORKER_SA_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com
g() { gcloud --project "$GCP_PROJECT" --quiet "$@"; }
step() { printf '\n==> %s\n' "$*"; }

step "APIs"
g services enable spanner.googleapis.com dataflow.googleapis.com pubsub.googleapis.com \
  storage.googleapis.com compute.googleapis.com iam.googleapis.com

step "Bucket gs://$BUCKET"
g storage buckets describe "gs://$BUCKET" >/dev/null 2>&1 ||
  g storage buckets create "gs://$BUCKET" --location "$REGION" --uniform-bucket-level-access

step "Spanner instance $SPANNER_INSTANCE ($SPANNER_UNITS processing units, regional-$REGION)"
g spanner instances describe "$SPANNER_INSTANCE" >/dev/null 2>&1 ||
  g spanner instances create "$SPANNER_INSTANCE" --config "regional-$REGION" \
    --processing-units "$SPANNER_UNITS" --description "crdb2spanner test" --edition STANDARD

step "Pub/Sub topic + subscription for bucket notifications (torture/cdc/)"
g pubsub topics describe "$TOPIC" >/dev/null 2>&1 || g pubsub topics create "$TOPIC"
g pubsub subscriptions describe "$SUB" >/dev/null 2>&1 ||
  g pubsub subscriptions create "$SUB" --topic "$TOPIC" --ack-deadline 120 --message-retention-duration 7d
if ! g storage buckets notifications list "gs://$BUCKET" --format='value(topic)' 2>/dev/null | grep -q "/topics/$TOPIC\$"; then
  # gcloud also grants the Cloud Storage service agent permission to publish to the topic
  g storage buckets notifications create "gs://$BUCKET" --topic "$TOPIC" \
    --event-types OBJECT_FINALIZE --object-prefix torture/cdc/ --payload-format json
fi

step "Dataflow worker service account $WORKER_SA"
g iam service-accounts describe "$WORKER_SA" >/dev/null 2>&1 ||
  g iam service-accounts create "$WORKER_SA_NAME" --display-name "crdb2spanner Dataflow workers"
for role in roles/dataflow.worker roles/spanner.databaseUser roles/pubsub.subscriber roles/pubsub.viewer; do
  g projects add-iam-policy-binding "$GCP_PROJECT" --member "serviceAccount:$WORKER_SA" --role "$role" \
    --condition None >/dev/null
done
g storage buckets add-iam-policy-binding "gs://$BUCKET" --member "serviceAccount:$WORKER_SA" \
  --role roles/storage.objectAdmin >/dev/null

step "This VM's service account (runs CockroachDB EXPORT/changefeeds, the CLI and submits Dataflow jobs)"
# From Cloud Shell or a laptop, name the VM's service account: VM_SA=123-compute@developer.gserviceaccount.com
ON_VM=""
if [ -z "${VM_SA:-}" ]; then
  VM_SA=$(curl -sf -H 'Metadata-Flavor: Google' \
    http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email || true)
  [ -n "$VM_SA" ] && ON_VM=1
fi
if [ -n "$VM_SA" ] && [[ "$VM_SA" != *.gserviceaccount.com ]]; then
  echo "metadata server returned '$VM_SA', not a service account (Cloud Shell?): set VM_SA to the VM's service account"
  VM_SA=""
fi
if [ -n "$VM_SA" ]; then
  for role in roles/spanner.databaseAdmin roles/dataflow.developer roles/pubsub.viewer; do
    g projects add-iam-policy-binding "$GCP_PROJECT" --member "serviceAccount:$VM_SA" --role "$role" \
      --condition None >/dev/null
  done
  g storage buckets add-iam-policy-binding "gs://$BUCKET" --member "serviceAccount:$VM_SA" \
    --role roles/storage.objectAdmin >/dev/null
  g iam service-accounts add-iam-policy-binding "$WORKER_SA" --member "serviceAccount:$VM_SA" \
    --role roles/iam.serviceAccountUser >/dev/null
  echo "granted roles to $VM_SA"
  SCOPES=$(curl -sf -H 'Metadata-Flavor: Google' \
    http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/scopes || true)
  if [ -n "$ON_VM" ] && ! grep -q "cloud-platform" <<<"$SCOPES"; then
    echo "WARNING: this VM's access scopes do not include cloud-platform, so CockroachDB cannot write to"
    echo "Cloud Storage and the CLI cannot reach Spanner. Stop the VM, then:"
    echo "  gcloud compute instances set-service-account <vm> --zone <zone> --scopes cloud-platform"
  fi
else
  echo "not on a GCE VM: grant your own identity the roles above and use 'gcloud auth application-default login'"
fi

step "Done"
cat <<EOF
export GCP_PROJECT=$GCP_PROJECT REGION=$REGION BUCKET=$BUCKET SPANNER_INSTANCE=$SPANNER_INSTANCE
export WORKER_SA=$WORKER_SA
Next: ./scripts/e2e_gcp.sh      Afterwards: ./scripts/gcp_teardown.sh
EOF
