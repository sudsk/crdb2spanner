"""Bulk load: snapshot Parquet (EXPORT ... AS OF SYSTEM TIME T0) -> Spanner.

Run locally (DirectRunner) or on Dataflow:

    crdb2spanner load --config job.yaml -- --runner DataflowRunner --project P \
        --region europe-west2 --temp_location gs://bkt/tmp --extra_package dist/crdb2spanner-*.tar.gz
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from ..config import JobConfig
from ..schema.mapping import MappingSpec
from .common.io import ShardAndBatch, WriteDlq
from .common.ops import DLQ, RowToOp
from .common.spanner_sink import SpannerApply


def build(p: beam.Pipeline, cfg: JobConfig, mapping: MappingSpec, read_path: str, tables: list[str]):
    mapping_json = mapping.to_json()
    dlqs = []
    for table in tables:
        pattern = f"{read_path.rstrip('/')}/{table}/*.parquet"
        rows = p | f"Read[{table}]" >> beam.io.ReadFromParquet(pattern)
        ops = rows | f"ToOp[{table}]" >> beam.ParDo(RowToOp(mapping_json, table)).with_outputs(DLQ, main="ops")
        dlqs.append(ops[DLQ])
        written = (
            ops.ops
            | f"Batch[{table}]" >> ShardAndBatch(cfg.apply.num_shards, cfg.apply.batch_size)
            | f"Write[{table}]" >> beam.ParDo(SpannerApply(
                cfg.target.project, cfg.target.instance, cfg.target.database, mapping_json,
                mode="bulk", emulator_host=cfg.target.emulator_host)).with_outputs(DLQ, main="ok")
        )
        dlqs.append(written[DLQ])
    (dlqs | "FlattenDlq" >> beam.Flatten() | "Dlq" >> WriteDlq(cfg.apply, "bulk", streaming=False))


def run(cfg: JobConfig, mapping: MappingSpec, read_path: str, tables: list[str], beam_args: list[str]):
    opts = PipelineOptions(beam_args, save_main_session=False)
    with beam.Pipeline(options=opts) as p:
        build(p, cfg, mapping, read_path, tables)
