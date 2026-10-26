"""Job configuration (one YAML file per migration job).

Values of the form ``${VAR}`` or ``${VAR:-default}`` are expanded from the environment,
so secrets never need to live in the file. ``dsn_secret`` may point at a Secret Manager
secret version instead of an inline DSN.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import yaml

_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _expand(value):
    if isinstance(value, str):
        return _ENV_RE.sub(lambda m: os.environ.get(m.group(1), m.group(2) or ""), value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


@dataclass
class SourceConfig:
    database: str
    dsn: str | None = None  # postgresql://user:pass@host:26257/db?sslmode=verify-full
    dsn_secret: str | None = None  # projects/<p>/secrets/<name>/versions/latest
    schema: str = "public"

    def resolve_dsn(self) -> str:
        if self.dsn:
            return self.dsn
        if self.dsn_secret:
            from google.cloud import secretmanager  # optional dependency

            client = secretmanager.SecretManagerServiceClient()
            return client.access_secret_version(name=self.dsn_secret).payload.data.decode()
        raise ValueError("source.dsn or source.dsn_secret must be set")


@dataclass
class TargetConfig:
    project: str
    instance: str
    database: str
    emulator_host: str | None = None  # e.g. localhost:9010 - sets SPANNER_EMULATOR_HOST

    def apply_emulator_env(self) -> None:
        if self.emulator_host:
            os.environ["SPANNER_EMULATOR_HOST"] = self.emulator_host


@dataclass
class CdcConfig:
    sink: Literal["pubsub", "files"] = "pubsub"
    # pubsub sink
    pubsub_project: str | None = None
    pubsub_region: str | None = None
    topic_prefix: str = "crdb2spanner-"
    subscription_suffix: str = "-sub"
    # files sink: URI the *CockroachDB cluster* writes to (nodelocal://1/cdc, gs://bkt/cdc?AUTH=implicit)
    files_sink_uri: str | None = None
    # ...and the path the *pipeline* reads the same files from (local dir or gs://bkt/cdc)
    files_read_path: str | None = None
    resolved_interval: str = "10s"
    min_checkpoint_frequency: str = "10s"
    sink_credentials_b64: str | None = None  # for AUTH=specified; otherwise AUTH=implicit
    extra_options: dict[str, str] = field(default_factory=dict)


@dataclass
class SnapshotConfig:
    # URI the CockroachDB cluster EXPORTs to, and the path the pipeline reads from
    export_uri: str = "nodelocal://1/snapshot"
    read_path: str = "./.local/extern/snapshot"


@dataclass
class ApplyConfig:
    mode: Literal["strict", "fast"] = "strict"
    shadow_prefix: str = "c2s_shadow_"
    num_shards: int = 64
    batch_size: int = 500
    max_batch_latency_s: int = 5
    dlq_path: str = "./.local/dlq"
    dlq_topic: str | None = None


@dataclass
class JobConfig:
    job_id: str
    source: SourceConfig
    target: TargetConfig
    tables: list[str] = field(default_factory=list)  # empty = all tables
    exclude_tables: list[str] = field(default_factory=list)
    cdc: CdcConfig = field(default_factory=CdcConfig)
    snapshot: SnapshotConfig = field(default_factory=SnapshotConfig)
    apply: ApplyConfig = field(default_factory=ApplyConfig)
    work_dir: str = "./.local/jobs"
    uuid_native: bool = False  # map UUID -> uuid instead of varchar(36)

    @property
    def job_dir(self) -> Path:
        p = Path(self.work_dir) / self.job_id
        p.mkdir(parents=True, exist_ok=True)
        return p

    def selects(self, table: str) -> bool:
        if table in self.exclude_tables:
            return False
        return not self.tables or table in self.tables


def load_job_config(path: str | os.PathLike) -> JobConfig:
    raw = _expand(yaml.safe_load(Path(path).read_text()))
    return JobConfig(
        job_id=raw["job_id"],
        source=SourceConfig(**raw["source"]),
        target=TargetConfig(**raw["target"]),
        tables=raw.get("tables") or [],
        exclude_tables=raw.get("exclude_tables") or [],
        cdc=CdcConfig(**(raw.get("cdc") or {})),
        snapshot=SnapshotConfig(**(raw.get("snapshot") or {})),
        apply=ApplyConfig(**(raw.get("apply") or {})),
        work_dir=raw.get("work_dir", "./.local/jobs"),
        uuid_native=bool(raw.get("uuid_native", False)),
    )
