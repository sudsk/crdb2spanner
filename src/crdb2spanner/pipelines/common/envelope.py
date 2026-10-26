"""CockroachDB changefeed message parsing (JSON format, wrapped envelope).

Handles the shapes emitted by the Pub/Sub and cloud-storage sinks:

  data   {"after": {...}|null, "key": [...], "updated": "<hlc>", "mvcc_timestamp": "<hlc>"}
         Pub/Sub may wrap it: {"key": [...], "value": {"after": ...}, "topic": "t"}
         (field names are matched case-insensitively)
  resolved  {"resolved": "<hlc>"}

HLC timestamps are decimal strings ``<wall nanos>.<10-digit logical>``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True, order=True)
class HLC:
    wall: int
    logical: int = 0

    @classmethod
    def parse(cls, s: str | Decimal | int) -> "HLC":
        s = str(s).strip()
        if "." in s:
            w, lg = s.split(".", 1)
            return cls(int(w), int(lg or 0))
        return cls(int(s), 0)

    def __str__(self) -> str:
        return f"{self.wall}.{self.logical:010d}"

    @property
    def seconds(self) -> float:
        return self.wall / 1e9


@dataclass(frozen=True)
class ChangeEvent:
    table: str
    key: tuple
    after: dict | None  # None => delete
    hlc: HLC

    @property
    def is_delete(self) -> bool:
        return self.after is None

    def key_id(self) -> str:
        """Stable string identity of (table, pk) used as the Beam state key."""
        return self.table + "|" + json.dumps(list(self.key), default=str, separators=(",", ":"))


@dataclass(frozen=True)
class ResolvedEvent:
    table: str
    hlc: HLC


class EnvelopeError(ValueError):
    pass


def _ci(d: dict[str, Any], name: str, default=None):
    if name in d:
        return d[name]
    low = name.lower()
    for k, v in d.items():
        if k.lower() == low:
            return v
    return default


def table_from_topic(topic: str, topic_prefix: str = "") -> str:
    t = topic
    if topic_prefix and t.startswith(topic_prefix):
        t = t[len(topic_prefix):]
    return t.split(".")[-1]


def parse_message(data: bytes | str, table: str | None = None, topic_prefix: str = ""):
    """Parse one changefeed message -> ChangeEvent | ResolvedEvent.

    ``table`` is required when the message itself does not carry a topic (files sink).
    Numbers are parsed as Decimal so DECIMAL / large values keep full precision.
    """
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8")
    try:
        msg = json.loads(data, parse_float=Decimal)
    except json.JSONDecodeError as e:
        raise EnvelopeError(f"invalid JSON: {e}") from e
    if not isinstance(msg, dict):
        raise EnvelopeError("message is not a JSON object")

    resolved = _ci(msg, "resolved")
    if resolved is not None:
        return ResolvedEvent(table or "", HLC.parse(resolved))

    value = _ci(msg, "value")
    body = value if isinstance(value, dict) else msg
    topic = _ci(msg, "topic") or _ci(body, "topic")
    tbl = table or (table_from_topic(topic, topic_prefix) if topic else None)
    if not tbl:
        raise EnvelopeError("cannot determine table (no topic and no table hint)")

    key = _ci(msg, "key")
    if key is None:
        key = _ci(body, "key")
    if not isinstance(key, list):
        raise EnvelopeError("message has no key array (enable key in value / use wrapped envelope)")

    ts = _ci(body, "updated") or _ci(body, "mvcc_timestamp")
    if ts is None:
        crdb = _ci(body, "crdb") or {}
        ts = _ci(crdb, "updated") or _ci(crdb, "mvcc_timestamp")
    if ts is None:
        raise EnvelopeError("message has no updated/mvcc_timestamp (create changefeed WITH updated)")

    after = _ci(body, "after")
    if after is not None and not isinstance(after, dict):
        raise EnvelopeError("'after' is not an object")
    return ChangeEvent(tbl, tuple(key), after, HLC.parse(ts))
