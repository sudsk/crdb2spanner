# Dataflow Flex Template image for crdb2spanner pipelines (M3 - not yet exercised).
#   gcloud builds submit --tag $REGION-docker.pkg.dev/$PROJECT/c2s/crdb2spanner:0.1 .
#   gcloud dataflow flex-template build gs://$BUCKET/templates/cdc_apply.json \
#     --image $REGION-docker.pkg.dev/$PROJECT/c2s/crdb2spanner:0.1 --sdk-language PYTHON \
#     --metadata-file templates/cdc_apply.metadata.json
FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

WORKDIR /template
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

COPY templates/flex_main.py ./flex_main.py
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/flex_main.py
