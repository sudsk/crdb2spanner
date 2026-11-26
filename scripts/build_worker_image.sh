#!/usr/bin/env bash
# Build the Dataflow worker image (templates/Dockerfile.worker) with Cloud Build and push it to
# Artifact Registry in the same project. Needed when Dataflow workers have no route to PyPI.
#
#   export GCP_PROJECT=my-project REGION=europe-west2
#   ./scripts/build_worker_image.sh            # prints the image URI
#   export C2S_WORKER_IMAGE=<printed URI>      # load/replicate on Dataflow then use the image
#
# Python and Beam versions are taken from the current environment (the one that will submit the
# jobs), because Dataflow requires the workers' SDK to match the submitter's.
# Options: REPO (default c2s), TAG (default <package version>-py<X.Y>-beam<version>),
#          LOCAL_DOCKER=1 to build with local docker instead of Cloud Build.
set -euo pipefail
cd "$(dirname "$0")/.."

: "${GCP_PROJECT:?set GCP_PROJECT}"
REGION=${REGION:-europe-west2}
REPO=${REPO:-c2s}
PY=$(python3 -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')
BEAM=$(python3 -c 'import apache_beam; print(apache_beam.__version__)')
VER=$(python3 -c 'import importlib.metadata as m; print(m.version("crdb2spanner"))' 2>/dev/null || echo dev)
TAG=${TAG:-${VER//+/-}-py${PY}-beam${BEAM}}
IMAGE="$REGION-docker.pkg.dev/$GCP_PROJECT/$REPO/worker:$TAG"
g() { gcloud --project "$GCP_PROJECT" --quiet "$@"; }

echo "==> image $IMAGE (Python $PY, Beam $BEAM)"
g services enable artifactregistry.googleapis.com cloudbuild.googleapis.com
g artifacts repositories describe "$REPO" --location "$REGION" >/dev/null 2>&1 ||
  g artifacts repositories create "$REPO" --location "$REGION" --repository-format docker \
    --description "crdb2spanner images"

if [ -n "${LOCAL_DOCKER:-}" ]; then
  gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet
  docker build -f templates/Dockerfile.worker --build-arg "PY=$PY" --build-arg "BEAM_VERSION=$BEAM" -t "$IMAGE" .
  docker push "$IMAGE"
else
  CB=$(mktemp)
  cat > "$CB" <<EOF
steps:
- name: gcr.io/cloud-builders/docker
  args: [build, -f, templates/Dockerfile.worker, --build-arg, PY=$PY, --build-arg, BEAM_VERSION=$BEAM, -t, $IMAGE, .]
images: [$IMAGE]
EOF
  # gcloud skips what .gitignore lists (.venv, .local, ...) when no .gcloudignore exists
  g builds submit --region "$REGION" --config "$CB" .
  rm -f "$CB"
fi

echo
echo "export C2S_WORKER_IMAGE=$IMAGE"
