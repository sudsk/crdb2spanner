import datetime as dt
from decimal import Decimal

import pytest

from crdb2spanner.schema.types import map_type
from crdb2spanner.schema.values import bit_reverse_64, canonical, get_transform


@pytest.mark.parametrize("src,expected", [
    ("INT8", "bigint"), ("INT4", "bigint"), ("STRING", "varchar"), ("STRING(50)", "varchar(50)"),
    ("VARCHAR(10)", "varchar(10)"), ("DECIMAL(10,2)", "numeric"), ("TIMESTAMPTZ", "timestamptz"),
    ("TIMESTAMP", "timestamptz"), ("JSONB", "jsonb"), ("BYTES", "bytea"), ("BOOL", "boolean"),
    ("UUID", "varchar(36)"), ("STRING[]", "varchar[]"), ("INT8[]", "bigint[]"),
    ("INTERVAL", "varchar(64)"), ("FLOAT8", "double precision"), ("DATE", "date"),
])
def test_map_type(src, expected):
    assert map_type(src).spanner_type == expected


def test_enum_and_unknown():
    assert map_type("event_kind", {"event_kind": ["a"]}).spanner_type == "varchar(255)"
    m = map_type("SOMETHING_NEW")
    assert m.severity == "high" and m.spanner_type == "varchar"


def test_uuid_native_option():
    assert map_type("UUID", uuid_native=True).spanner_type == "uuid"


def test_bit_reverse_is_involution_and_spreads():
    vals = [1, 2, 3, 900000000000000001, 900000000000000002]
    rev = [bit_reverse_64(v) for v in vals]
    assert [bit_reverse_64(r) for r in rev] == vals
    assert len(set(rev)) == len(vals)
    # consecutive ids end up far apart in key space
    assert abs(rev[3] - rev[4]) > 2 ** 60


def test_transforms():
    ts = get_transform("timestamp")("2024-05-01T10:11:12.123456789Z")
    assert ts == dt.datetime(2024, 5, 1, 10, 11, 12, 123456, tzinfo=dt.timezone.utc)
    naive = get_transform("timestamp_naive_utc")("2024-05-01 10:11:12")
    assert naive.tzinfo == dt.timezone.utc
    assert get_transform("bytes")("\\x6869") == b"hi"
    assert get_transform("decimal")(Decimal("10.50")) == Decimal("10.50")
    assert get_transform("uuid")("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11") == "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    assert get_transform("array:int")(["1", 2, None]) == [1, 2, None]
    assert get_transform("json")('{"b":1,"a":[1,2]}') == {"b": 1, "a": [1, 2]}
    assert get_transform("int")(None) is None


def test_canonical_normalises_equivalent_values():
    assert canonical(Decimal("1.50")) == canonical(Decimal("1.5"))
    a = dt.datetime(2024, 1, 1, 12, tzinfo=dt.timezone.utc)
    b = dt.datetime(2024, 1, 1, 13, tzinfo=dt.timezone(dt.timedelta(hours=1)))
    assert canonical(a) == canonical(b)
    j1 = get_transform("json")({"b": 1, "a": 2})
    j2 = get_transform("json")('{"a":2,"b":1}')
    assert canonical(j1) == canonical(j2)
    assert canonical(None) != canonical("")
