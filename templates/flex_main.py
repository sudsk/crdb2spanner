"""Flex Template entry point (M3). Job config + mapping are read from GCS.

    --pipeline cdc_apply|bulk_load --job_config gs://.../job.yaml --mapping gs://.../mapping.json
"""

import argparse
import json
import tempfile

from apache_beam.io.filesystems import FileSystems

from crdb2spanner.config import load_job_config
from crdb2spanner.schema.mapping import MappingSpec


def _fetch(uri: str) -> str:
    with FileSystems.open(uri) as f:
        return f.read().decode()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pipeline", required=True, choices=["cdc_apply", "bulk_load"])
    ap.add_argument("--job_config", required=True)
    ap.add_argument("--mapping", required=True)
    ap.add_argument("--tables", default="")
    ap.add_argument("--source", default=None)
    args, beam_args = ap.parse_known_args()

    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        f.write(_fetch(args.job_config))
    cfg = load_job_config(f.name)
    mapping = MappingSpec.from_dict(json.loads(_fetch(args.mapping)))
    tables = [t for t in args.tables.split(",") if t] or [t.source for t in mapping.tables if cfg.selects(t.source)]

    if args.pipeline == "bulk_load":
        from crdb2spanner.pipelines import bulk_load
        bulk_load.run(cfg, mapping, cfg.snapshot.read_path, tables, beam_args)
    else:
        from crdb2spanner.pipelines import cdc_apply
        cdc_apply.run(cfg, mapping, tables, beam_args, source=args.source or cfg.cdc.sink)


if __name__ == "__main__":
    main()
