"""Value transforms: CockroachDB value (from changefeed JSON or Parquet) -> Spanner client value.

All transforms are tolerant of both encodings:
  * changefeed JSON  - everything arrives as JSON scalars / strings
  * EXPORT Parquet   - native int/float/Decimal/datetime/bytes/str, arrays as lists
"""

from __future__ import annotations

import base64
import binascii
import datetime as dt
import json
import math
import uuid as _uuid
from decimal import Decimal
from typing import Any, Callable

from google.cloud.spanner_v1.data_types import JsonObject


def _identity(v):
    return v


def _to_int(v):
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int,)):
        return v
    if isinstance(v, float):
        return int(v)
    return int(str(v))


def _to_float(v):
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v)
    low = s.lower()
    if low in ("nan",):
        return math.nan
    if low in ("inf", "+inf", "infinity"):
        return math.inf
    if low in ("-inf", "-infinity"):
        return -math.inf
    return float(s)


def _to_decimal(v):
    if isinstance(v, Decimal):
        return v
    if isinstance(v, float):
        return Decimal(repr(v))
    return Decimal(str(v))


def _to_bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() in ("t", "true", "1", "yes")
    return bool(v)


def _to_bytes(v):
    if isinstance(v, (bytes, bytearray)):
        return bytes(v)
    s = str(v)
    if s.startswith("\\x"):
        return binascii.unhexlify(s[2:])
    try:
        return base64.b64decode(s, validate=True)
    except (binascii.Error, ValueError):
        return s.encode()


def _to_date(v):
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    return dt.date.fromisoformat(str(v)[:10])


def _parse_ts(v) -> dt.datetime:
    if isinstance(v, dt.datetime):
        return v
    s = str(v).strip().replace(" ", "T", 1)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    # Python <3.11 cannot parse >6 fractional digits; truncate nanos to micros
    if "." in s:
        head, frac = s.split(".", 1)
        digits = "".join(ch for ch in frac if ch.isdigit())
        tail = frac[len(digits):]
        s = f"{head}.{digits[:6].ljust(6, '0')}{tail}"
    # +00 -> +00:00
    if len(s) >= 3 and s[-3] in "+-" and s[-2:].isdigit():
        s = s + ":00"
    return dt.datetime.fromisoformat(s)


def _to_timestamp(v):
    ts = _parse_ts(v)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc)


def _to_timestamp_naive_utc(v):
    ts = _parse_ts(v)
    return ts.replace(tzinfo=dt.timezone.utc) if ts.tzinfo is None else ts.astimezone(dt.timezone.utc)


def _to_json(v):
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except json.JSONDecodeError:
            pass
    return JsonObject(v)


def _to_uuid(v):
    if isinstance(v, (bytes, bytearray)) and len(v) == 16:
        return str(_uuid.UUID(bytes=bytes(v)))
    return str(_uuid.UUID(str(v)))


def _to_text(v):
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="replace")
    if isinstance(v, (dt.time, dt.datetime, dt.date)):
        return v.isoformat()
    if isinstance(v, dt.timedelta):
        return str(v)
    return str(v)


def bit_reverse_64(v) -> int:
    """Reverse the 64 bits of a (signed) INT8. Distributes monotonically increasing
    unique_rowid() values across the key space while staying unique and reversible."""
    n = _to_int(v) & 0xFFFFFFFFFFFFFFFF
    r = int(f"{n:064b}"[::-1], 2)
    return r - (1 << 64) if r >= (1 << 63) else r


_SCALARS: dict[str, Callable[[Any], Any]] = {
    "identity": _identity,
    "int": _to_int,
    "float": _to_float,
    "decimal": _to_decimal,
    "bool": _to_bool,
    "bytes": _to_bytes,
    "date": _to_date,
    "timestamp": _to_timestamp,
    "timestamp_naive_utc": _to_timestamp_naive_utc,
    "json": _to_json,
    "uuid": _to_uuid,
    "to_text": _to_text,
    "bit_reverse": bit_reverse_64,
}


def get_transform(name: str) -> Callable[[Any], Any]:
    if name.startswith("array:"):
        inner = get_transform(name.split(":", 1)[1])

        def _arr(v):
            if v is None:
                return None
            if isinstance(v, str):
                v = json.loads(v)
            return [None if x is None else inner(x) for x in v]

        return _arr
    fn = _SCALARS[name]

    def _nullsafe(v):
        return None if v is None else fn(v)

    return _nullsafe


# ------------------------------------------------------------------------------------
# Canonical form for validation hashing
# ------------------------------------------------------------------------------------

def canonical(v: Any) -> str:
    """Stable, type-normalised string used to hash rows identically on both sides."""
    if v is None:
        return "\x00"
    if isinstance(v, bool):
        return "t" if v else "f"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if math.isnan(v):
            return "nan"
        return repr(v)
    if isinstance(v, Decimal):
        if v.is_nan():
            return "nan"
        return format(v.normalize(), "f")
    if isinstance(v, dt.datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=dt.timezone.utc)
        return v.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
    if isinstance(v, dt.date):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return "\\x" + bytes(v).hex()
    if isinstance(v, JsonObject):
        s = v.serialize()
        v = None if s is None else json.loads(s)
        return json.dumps(v, sort_keys=True, separators=(",", ":"), default=str)
    if isinstance(v, dict):
        return json.dumps(v, sort_keys=True, separators=(",", ":"), default=str)
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(canonical(x) for x in v) + "]"
    return str(v)
