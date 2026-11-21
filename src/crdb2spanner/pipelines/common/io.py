"""Shared pipeline plumbing: sharded batching and DLQ writers."""

from __future__ import annotations

import logging
import os
import zlib

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.transforms.window import FixedWindows

from ...config import ApplyConfig
from .ops import Op

log = logging.getLogger(__name__)


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


LOCAL_RUNNERS = (None, "DirectRunner", "SwitchingDirectRunner")


def pick_local_runner(opts, streaming: bool) -> str | None:
    """Run local batch pipelines on the FnApiRunner.

    Without ``--runner`` Beam's DirectRunner decides at run time and, depending on the Python
    and Beam versions, can fall back to the legacy BundleBasedDirectRunner, whose GroupByKey
    fails with an AssertionError on some versions. Batch pipelines here need nothing that
    runner offers, so pin the FnApiRunner. ``C2S_LOCAL_RUNNER`` overrides; Dataflow and other
    explicit runners are left alone. Streaming keeps the DirectRunner (FnApiRunner is batch-only).
    """
    from apache_beam.options.pipeline_options import StandardOptions

    so = opts.view_as(StandardOptions)
    if so.runner not in LOCAL_RUNNERS:
        return so.runner
    override = os.environ.get("C2S_LOCAL_RUNNER")
    if override:
        so.runner = override
    elif not streaming:
        so.runner = "FnApiRunner"
    return so.runner


_PACKAGING_FLAGS = ("--extra_package", "--extra_packages", "--setup_file", "--sdk_container_image",
                    "--requirements_file", "--flexrs_goal")


def ship_package(beam_args: list[str]) -> list[str]:
    """Dataflow workers need this package. When submitting to Dataflow from a source checkout with
    no packaging flag of your own, build a wheel of the checkout and add ``--extra_package``."""
    import subprocess
    import sys
    import tempfile
    from pathlib import Path

    joined = " ".join(beam_args)
    if "DataflowRunner" not in joined or any(f in joined for f in _PACKAGING_FLAGS[:-1]):
        return beam_args
    root = Path(__file__).resolve().parents[4]
    if not (root / "pyproject.toml").exists():
        log.warning("not running from a source checkout: pass --extra_package or --sdk_container_image "
                    "so Dataflow workers have crdb2spanner")
        return beam_args
    out = Path(tempfile.mkdtemp(prefix="c2s-wheel-"))
    subprocess.run([sys.executable, "-m", "pip", "wheel", str(root), "--no-deps", "-q", "-w", str(out)], check=True)
    wheel = next(out.glob("crdb2spanner-*.whl"))
    log.info("shipping %s to Dataflow workers", wheel.name)
    return [*beam_args, "--extra_package", str(wheel)]
