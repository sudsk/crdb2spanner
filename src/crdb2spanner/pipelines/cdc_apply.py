"""CDC apply: CockroachDB changefeed messages -> Spanner.

Sources
  pubsub  one subscription per table (streaming)
  files   changefeed cloud-storage sink output (gs:// or local nodelocal dir);
          batch replay by default, ``follow=True`` to watch for new files (streaming)

Flow (ARCHITECTURE.md 4.6)
  parse -> [resolved -> watermark metrics]
        -> data -> latest-per-key (stateful LWW in streaming / CombinePerKey in batch)
        -> mapping transform -> shard + batch -> SpannerApply(strict|fast) -> DLQ
"""

from __future__ import annotations

import logging
import os

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from ..config import JobConfig
from ..schema.mapping import MappingSpec
from .common.envelope import ChangeEvent, EnvelopeError, ResolvedEvent, parse_message
from .common.io import ShardAndBatch, WriteDlq, pick_local_runner
from .common.lww import LatestWinsDoFn
from .common.ops import DLQ, EventToOp, dlq_record
from .common.spanner_sink import SpannerApply

log = logging.getLogger(__name__)
RESOLVED = "resolved"


class ParseMessage(beam.DoFn):
    def __init__(self, table: str, topic_prefix: str = ""):
        self.table, self.topic_prefix = table, topic_prefix
        self.bad = Metrics.counter("cdc", "parse_errors")

    def process(self, data):
        try:
            ev = parse_message(data, table=self.table, topic_prefix=self.topic_prefix)
        except EnvelopeError as e:
            self.bad.inc()
            yield beam.pvalue.TaggedOutput(DLQ, dlq_record(
                "parse", data.decode("utf-8", "replace") if isinstance(data, bytes) else data, e))
            return
        if isinstance(ev, ResolvedEvent):
            yield beam.pvalue.TaggedOutput(RESOLVED, ev)
        else:
            yield ev


class TrackResolved(beam.DoFn):
    """Exports per-table resolved timestamp (ms) as a gauge; M2 persists it for cutover gating."""

    def process(self, ev: ResolvedEvent):
        Metrics.gauge("cdc_resolved_ms", ev.table).set(ev.hlc.wall // 1_000_000)
        log.info("resolved %s %s", ev.table, ev.hlc)
        yield ev


def readable(path: str) -> bool:
    """Local files written by a container may still be root-only; object stores are always readable."""
    if "://" in path and not path.startswith("file://"):
        return True
    return os.access(path.replace("file://", "", 1), os.R_OK)


class SkipUnreadable(beam.DoFn):
    """Drop files the pipeline user cannot read yet (written after the flush point; the next replay
    or the watcher picks them up) instead of failing the whole run."""

    def __init__(self):
        self.skipped = Metrics.counter("cdc", "files_skipped_unreadable")

    def process(self, md):
        if readable(md.path):
            yield md
        else:
            self.skipped.inc()
            # RESOLVED files only carry a checkpoint timestamp (no rows) and are written
            # continuously, so a just-created one is routine; only data files merit a warning
            level = logging.DEBUG if md.path.endswith(".RESOLVED") else logging.WARNING
            log.log(level, "skipping unreadable CDC file (will be picked up later): %s", md.path)


def _latest(events):
    events = list(events)
    return max(events, key=lambda e: e.hlc) if events else None


def _read_files_table(p, base: str, table: str, follow: bool, interval_s: int):
    """Changefeed cloud-storage sink: <base>/<table>/[YYYY-MM-DD/]<ts>-...-<topic>-<n>.ndjson
    plus <ts>.RESOLVED files."""
    root = f"{base.rstrip('/')}/{table}"
    patterns = [f"{root}/*", f"{root}/*/*"]
    matches = []
    for i, pat in enumerate(patterns):
        if follow:
            m = p | f"Watch[{table}/{i}]" >> fileio.MatchContinuously(pat, interval=interval_s)
        else:
            m = p | f"Match[{table}/{i}]" >> fileio.MatchFiles(pat, empty_match_treatment=fileio.EmptyMatchTreatment.ALLOW)
        matches.append(m)
    return (
        matches
        | f"FlatMatches[{table}]" >> beam.Flatten()
        | f"OnlyCdcFiles[{table}]" >> beam.Filter(
            lambda md: md.path.endswith(".ndjson") or md.path.endswith(".RESOLVED")
            or md.path.endswith(".json"))
        | f"Readable[{table}]" >> beam.ParDo(SkipUnreadable())
        | f"Open[{table}]" >> fileio.ReadMatches()
        | f"Lines[{table}]" >> beam.FlatMap(
            lambda f: [ln for ln in f.read_utf8().splitlines() if ln.strip()])
    )


def build(p: beam.Pipeline, cfg: JobConfig, mapping: MappingSpec, tables: list[str], *,
          source: str, streaming: bool, follow: bool = False, poll_interval_s: int = 10):
    mapping_json = mapping.to_json()
    events, dlqs, resolved = [], [], []

    for table in tables:
        if source == "pubsub":
            from apache_beam.io.gcp.pubsub import ReadFromPubSub

            sub = (f"projects/{cfg.cdc.pubsub_project}/subscriptions/"
                   f"{cfg.cdc.topic_prefix}{table}{cfg.cdc.subscription_suffix}")
            raw = p | f"PubSub[{table}]" >> ReadFromPubSub(subscription=sub)
        elif source == "files":
            if not cfg.cdc.files_read_path:
                raise ValueError("cdc.files_read_path is required for source=files")
            raw = _read_files_table(p, cfg.cdc.files_read_path, table, follow, poll_interval_s)
        else:
            raise ValueError(source)

        parsed = raw | f"Parse[{table}]" >> beam.ParDo(
            ParseMessage(table, cfg.cdc.topic_prefix)).with_outputs(DLQ, RESOLVED, main="events")
        events.append(parsed.events)
        dlqs.append(parsed[DLQ])
        resolved.append(parsed[RESOLVED])

    _ = (resolved | "FlattenResolved" >> beam.Flatten() | "TrackResolved" >> beam.ParDo(TrackResolved()))

    all_events = events | "FlattenEvents" >> beam.Flatten()
    keyed = all_events | "KeyByPk" >> beam.Map(lambda e: (e.key_id(), e))
    if streaming:
        latest = keyed | "LatestWins" >> beam.ParDo(LatestWinsDoFn())
    else:
        latest = (keyed | "GroupPk" >> beam.GroupByKey()
                  | "PickLatest" >> beam.Map(lambda kv: _latest(kv[1]))
                  | "NotNone" >> beam.Filter(lambda e: e is not None))

    ops = latest | "ToOps" >> beam.ParDo(EventToOp(mapping_json)).with_outputs(DLQ, main="ops")
    dlqs.append(ops[DLQ])

    written = (
        ops.ops
        | "ShardBatch" >> ShardAndBatch(cfg.apply.num_shards, cfg.apply.batch_size,
                                        cfg.apply.max_batch_latency_s if streaming else None)
        | "Apply" >> beam.ParDo(SpannerApply(
            cfg.target.project, cfg.target.instance, cfg.target.database, mapping_json,
            mode=cfg.apply.mode, emulator_host=cfg.target.emulator_host,
                max_commit_delay_ms=cfg.apply.max_commit_delay_ms, commit_timeout_s=cfg.apply.commit_timeout_s,
                max_mutations=cfg.apply.max_mutations_per_commit,
                transaction_tag=f"c2s-{cfg.job_id}")).with_outputs(DLQ, main="ok")
    )
    dlqs.append(written[DLQ])
    _ = dlqs | "FlattenDlq" >> beam.Flatten() | "Dlq" >> WriteDlq(cfg.apply, "cdc", streaming)


def count_cdc_files(base: str, tables: list[str]) -> dict[str, int]:
    """Number of changefeed files visible to the pipeline, per table (files source)."""
    from apache_beam.io.filesystems import FileSystems

    out = {}
    for t in tables:
        root = f"{base.rstrip('/')}/{t}"
        pats = [f"{root}/*", f"{root}/*/*"]
        res = FileSystems.match(pats)
        out[t] = sum(1 for r in res for m in r.metadata_list
                     if m.path.endswith((".ndjson", ".json", ".RESOLVED")) and readable(m.path))
    return out


def run(cfg: JobConfig, mapping: MappingSpec, tables: list[str], beam_args: list[str], *,
        source: str, follow: bool = False):
    if source == "files" and not follow:
        counts = count_cdc_files(cfg.cdc.files_read_path, tables)
        log.info("CDC files per table: %s", counts)
        print("CDC files per table: " + ", ".join(f"{t}={n}" for t, n in counts.items()))
        if sum(counts.values()) == 0:
            raise RuntimeError(
                f"no changefeed files found under {cfg.cdc.files_read_path} - check the changefeeds are "
                "running and that this user can read the directory (files written by a container may be "
                "root-only)")
    opts = PipelineOptions(beam_args, save_main_session=False)
    streaming = source == "pubsub" or follow
    if streaming:
        opts.view_as(StandardOptions).streaming = True
    pick_local_runner(opts, streaming)
    p = beam.Pipeline(options=opts)
    build(p, cfg, mapping, tables, source=source, streaming=streaming, follow=follow)
    result = p.run()
    if not streaming or opts.view_as(StandardOptions).runner in (None, "DirectRunner", "FnApiRunner", "PrismRunner"):
        result.wait_until_finish()
    return result


__all__ = ["build", "run", "ChangeEvent"]
