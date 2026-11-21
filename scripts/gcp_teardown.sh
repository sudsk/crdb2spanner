#!/usr/bin/env bash
# Remove what scripts/gcp_setup.sh created (Spanner is billed per hour while it exists).
#
#   ./scripts/gcp_teardown.sh            # Spanner instance, Dataflow jobs, Pub/Sub, notification
#   DELETE_BUCKET=1 ./scripts/gcp_teardown.sh   # ...and the bucket with everything in it
set -uo pipefail

: "${GCP_PROJECT:?set GCP_PROJECT}"
REGION=${REGION:-europe-west2}
BUCKET=${BUCKET:-${GCP_PROJECT}-c2s}
SPANNER_INSTANCE=${SPANNER_INSTANCE:-c2s-test}
g() { gcloud --project "$GCP_PROJECT" --quiet "$@"; }

echo "==> cancel running crdb2spanner Dataflow jobs"
for j in $(g dataflow jobs list --region "$REGION" --status active --format='value(id,name)' | awk '$2 ~ /c2s|crdb2spanner|beamapp/ {print $1}'); do
  g dataflow jobs cancel "$j" --region "$REGION" || true
done

echo "==> Spanner instance $SPANNER_INSTANCE"
g spanner instances delete "$SPANNER_INSTANCE" || true

echo "==> bucket notifications, Pub/Sub"
for n in $(g storage buckets notifications list "gs://$BUCKET" --format='value(name)' 2>/dev/null); do
  g storage buckets notifications delete "$n" || true
done
g pubsub subscriptions delete c2s-torture-cdc-files || true
g pubsub topics delete c2s-torture-cdc-files || true

if [ -n "${DELETE_BUCKET:-}" ]; then
  echo "==> bucket gs://$BUCKET"
  g storage rm -r "gs://$BUCKET" || true
fi
echo "done (service account c2s-dataflow and IAM bindings are kept; they cost nothing)"
