"""CockroachDB changefeed management (one changefeed per table, one topic/dir per table)."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from urllib.parse import quote

from ..config import CdcConfig


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def sink_uri(cdc: CdcConfig, table: str) -> str:
    if cdc.sink == "pubsub":
        if not cdc.pubsub_project:
            raise ValueError("cdc.pubsub_project is required for the pubsub sink")
        params = [f"topic_name={quote(cdc.topic_prefix + table)}"]
        if cdc.pubsub_region:
            params.append(f"region={quote(cdc.pubsub_region)}")
        if cdc.sink_credentials_b64:
            params += ["AUTH=specified", f"CREDENTIALS={quote(cdc.sink_credentials_b64)}"]
        else:
            params.append("AUTH=implicit")
        return f"gcpubsub://{cdc.pubsub_project}?" + "&".join(params)
    if cdc.sink == "files":
        if not cdc.files_sink_uri:
            raise ValueError("cdc.files_sink_uri is required for the files sink")
        base, _, qs = cdc.files_sink_uri.partition("?")
        uri = f"{base.rstrip('/')}/{table}"
        return f"{uri}?{qs}" if qs else uri
    raise ValueError(f"unknown sink {cdc.sink}")


def create_statement(database: str, schema: str, table: str, cdc: CdcConfig, cursor: str) -> str:
    opts = {
        "cursor": f"'{cursor}'",
        "updated": None,
        "mvcc_timestamp": None,
        "resolved": f"'{cdc.resolved_interval}'",
        "min_checkpoint_frequency": f"'{cdc.min_checkpoint_frequency}'",
        "on_error": "'pause'",
        "schema_change_policy": "'stop'",
        "format": "'json'",
    }
    for k, v in cdc.extra_options.items():
        opts[k] = f"'{v}'" if v not in (None, "") else None
    with_clause = ", ".join(k if v is None else f"{k} = {v}" for k, v in opts.items())
    target = f"{_q(database)}.{_q(schema)}.{_q(table)}"
    return f"CREATE CHANGEFEED FOR TABLE {target} INTO '{sink_uri(cdc, table)}' WITH {with_clause}"


@dataclass
class ChangefeedJob:
    job_id: int
    status: str
    description: str
    high_water: str | None

    @property
    def lag_seconds(self) -> float | None:
        if not self.high_water:
            return None
        wall_ns = int(str(self.high_water).split(".")[0])
        now = dt.datetime.now(dt.timezone.utc).timestamp()
        return max(0.0, now - wall_ns / 1e9)


def create(conn, database: str, schema: str, table: str, cdc: CdcConfig, cursor: str) -> int:
    cur = conn.cursor()
    cur.execute(create_statement(database, schema, table, cdc, cursor))
    return int(cur.fetchone()[0])


def list_jobs(conn, job_tag: str | None = None) -> list[ChangefeedJob]:
    cur = conn.cursor()
    cur.execute("SHOW CHANGEFEED JOBS")
    cols = [d.name for d in cur.description]
    jobs = []
    for row in cur.fetchall():
        r = dict(zip(cols, row))
        desc = r.get("description") or ""
        if job_tag and job_tag not in desc:
            continue
        jobs.append(ChangefeedJob(int(r["job_id"]), r["status"], desc,
                                  str(r["high_water_timestamp"]) if r.get("high_water_timestamp") else None))
    return jobs


def cancel(conn, job_id: int) -> None:
    conn.cursor().execute(f"CANCEL JOB {int(job_id)}")
