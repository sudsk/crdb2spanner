"""Shared pipeline plumbing: sharded batching and DLQ writers."""

from __future__ import annotations

import zlib

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.transforms.window import FixedWindows

from ...config import ApplyConfig
from .ops import Op


def shard_key(op: Op, num_shards: int) -> str:
    """All ops for one primary key land in the same shard (=> same batch stream)."""
    return f"{op.target_table}|{zlib.crc32(op.ident().encode()) % num_shards}"


class ShardAndBatch(beam.PTransform):
    def __init__(self, num_shards: int, batch_size: int, max_latency_s: int | None = None):
        super().__init__()
        self.num_shards, self.batch_size, self.max_latency_s = num_shards, batch_size, max_latency_s

    def expand(self, ops):
        n = self.num_shards
        keyed = ops | "ShardKey" >> beam.Map(lambda op: (shard_key(op, n), op))
        kwargs = {}
        if self.max_latency_s:
            kwargs["max_buffering_duration_secs"] = self.max_latency_s
        return keyed | "Batch" >> beam.GroupIntoBatches(self.batch_size, **kwargs)


class WriteDlq(beam.PTransform):
    def __init__(self, cfg: ApplyConfig, label: str, streaming: bool):
        super().__init__()
        self.cfg, self.prefix, self.streaming = cfg, label, streaming

    def expand(self, records):
        if self.cfg.dlq_topic:
            from apache_beam.io.gcp.pubsub import WriteToPubSub

            return (records | "Encode" >> beam.Map(lambda s: s.encode("utf-8"))
                    | "ToPubSub" >> WriteToPubSub(self.cfg.dlq_topic))
        if self.streaming:
            return (records
                    | "Window" >> beam.WindowInto(FixedWindows(60))
                    | "ToFiles" >> fileio.WriteToFiles(
                        path=self.cfg.dlq_path,
                        file_naming=fileio.default_file_naming(prefix=self.prefix, suffix=".jsonl"),
                        shards=1))
        return records | "ToText" >> beam.io.WriteToText(
            f"{self.cfg.dlq_path.rstrip('/')}/{self.prefix}", file_name_suffix=".jsonl")
