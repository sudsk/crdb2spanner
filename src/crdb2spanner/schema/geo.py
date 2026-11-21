"""Geospatial values: one stored form for every path.

CockroachDB renders GEOMETRY / GEOGRAPHY as EWKB hex in ``EXPORT`` and ``::STRING`` (snapshot and
validation), but as GeoJSON in changefeed JSON. Spanner has no spatial type, so the tool stores the
EWKB hex text; this module converts GeoJSON from the changefeed into the same EWKB hex.

EWKB layout written here (little-endian, as CockroachDB writes it):
  byte order (01) | uint32 type [| 0x80000000 if Z] [| 0x20000000 + uint32 SRID if SRID != 0] | body
"""

from __future__ import annotations

import json
import struct
from decimal import Decimal

_TYPES = {"Point": 1, "LineString": 2, "Polygon": 3, "MultiPoint": 4, "MultiLineString": 5,
          "MultiPolygon": 6, "GeometryCollection": 7}
_Z = 0x80000000
_SRID = 0x20000000


def _dims(geom: dict) -> int:
    def first(c):
        while isinstance(c, list) and c and isinstance(c[0], list):
            c = c[0]
        return c

    if geom["type"] == "GeometryCollection":
        gs = geom.get("geometries") or []
        return _dims(gs[0]) if gs else 2
    c = first(geom.get("coordinates") or [])
    return 3 if isinstance(c, list) and len(c) >= 3 else 2


def _pt(c, dims: int) -> bytes:
    vals = [float(x) for x in c[:dims]]
    if not vals:  # empty point: NaN coordinates (PostGIS / CockroachDB convention)
        vals = [float("nan")] * dims
    return struct.pack("<" + "d" * dims, *vals)


def _ring(cs, dims: int) -> bytes:
    return struct.pack("<I", len(cs)) + b"".join(_pt(c, dims) for c in cs)


def _wkb(geom: dict, srid: int, dims: int) -> bytes:
    t = geom["type"]
    code = _TYPES[t] | (_Z if dims == 3 else 0)
    head = b"\x01" + (struct.pack("<II", code | _SRID, srid) if srid else struct.pack("<I", code))
    c = geom.get("coordinates")
    if t == "Point":
        body = _pt(c or [], dims)
    elif t == "LineString":
        body = _ring(c, dims)
    elif t == "Polygon":
        body = struct.pack("<I", len(c)) + b"".join(_ring(r, dims) for r in c)
    elif t in ("MultiPoint", "MultiLineString", "MultiPolygon"):
        sub = {"MultiPoint": "Point", "MultiLineString": "LineString", "MultiPolygon": "Polygon"}[t]
        body = struct.pack("<I", len(c)) + b"".join(_wkb({"type": sub, "coordinates": x}, 0, dims) for x in c)
    else:
        gs = geom.get("geometries") or []
        body = struct.pack("<I", len(gs)) + b"".join(_wkb(g, 0, dims) for g in gs)
    return head + body


def _srid_of(geom: dict, default: int) -> int:
    crs = ((geom.get("crs") or {}).get("properties") or {}).get("name", "")
    if crs.upper().startswith("EPSG:"):
        return int(crs.split(":")[1])
    return default


def to_ewkb_hex(value, srid: int = 0) -> str:
    """GeoJSON (dict or text) -> EWKB hex; EWKB hex / bytes are normalised to upper-case hex."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex().upper()
    if isinstance(value, str):
        s = value.strip()
        if not s.startswith("{"):
            return s.upper()
        value = json.loads(s, parse_float=Decimal)
    if not isinstance(value, dict) or "type" not in value:
        raise ValueError(f"not a GeoJSON geometry: {value!r}"[:200])
    return _wkb(value, _srid_of(value, srid), _dims(value)).hex().upper()
