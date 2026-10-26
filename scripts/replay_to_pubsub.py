"""Publish changefeed file-sink output to Pub/Sub (emulator or real) to exercise the
pubsub source of the CDC pipeline locally - CockroachDB cannot publish to the emulator.

    export PUBSUB_EMULATOR_HOST=localhost:8085
    python scripts/replay_to_pubsub.py --config examples/job.local.yaml --project local-project
    crdb2spanner replicate -c examples/job.local.yaml --source pubsub -- --streaming
"""

from __future__ import annotations

import argparse
from pathlib import Path

from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1

from crdb2spanner.config import load_job_config


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--project", required=True)
    args = ap.parse_args()
    cfg = load_job_config(args.config)
    root = Path(cfg.cdc.files_read_path)

    pub, sub = pubsub_v1.PublisherClient(), pubsub_v1.SubscriberClient()
    for tdir in sorted(p for p in root.iterdir() if p.is_dir()):
        table = tdir.name
        topic = pub.topic_path(args.project, f"{cfg.cdc.topic_prefix}{table}")
        subscription = sub.subscription_path(args.project, f"{cfg.cdc.topic_prefix}{table}{cfg.cdc.subscription_suffix}")
        for create in (lambda: pub.create_topic(name=topic),
                       lambda: sub.create_subscription(name=subscription, topic=topic)):
            try:
                create()
            except AlreadyExists:
                pass
        n = 0
        for f in sorted(tdir.rglob("*")):
            if f.is_file() and (f.suffix in (".ndjson", ".json") or f.name.endswith(".RESOLVED")):
                for line in f.read_text().splitlines():
                    if line.strip():
                        pub.publish(topic, line.encode())
                        n += 1
        print(f"{table}: published {n} messages to {topic}")
    pub.stop()


if __name__ == "__main__":
    main()
