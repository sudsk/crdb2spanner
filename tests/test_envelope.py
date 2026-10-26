from decimal import Decimal

import pytest

from crdb2spanner.pipelines.common.envelope import (
    HLC,
    ChangeEvent,
    EnvelopeError,
    ResolvedEvent,
    parse_message,
)


def test_hlc_ordering_and_roundtrip():
    a = HLC.parse("1712345678901234567.0000000001")
    b = HLC.parse("1712345678901234567.0000000002")
    c = HLC.parse("1712345678901234568.0000000000")
    assert a < b < c
    assert HLC.parse(str(b)) == b
    assert HLC.parse("1712345678901234567") == HLC(1712345678901234567, 0)


def test_files_sink_upsert():
    line = ('{"after": {"city": "rome", "id": "3a3d", "revenue": 12.50}, "key": ["rome", "3a3d"], '
            '"updated": "1712345678901234567.0000000003", "mvcc_timestamp": "1712345678901234567.0000000003"}')
    ev = parse_message(line, table="rides")
    assert isinstance(ev, ChangeEvent)
    assert ev.table == "rides" and ev.key == ("rome", "3a3d") and not ev.is_delete
    assert ev.after["revenue"] == Decimal("12.50")  # no float precision loss
    assert ev.key_id() == 'rides|["rome","3a3d"]'


def test_pubsub_wrapped_and_case_insensitive():
    msg = b'{"Key": ["nyc", "u1"], "Value": {"after": {"name": "x"}, "updated": "10.0000000001"}, "Topic": "c2s-users"}'
    ev = parse_message(msg, topic_prefix="c2s-")
    assert ev.table == "users" and ev.key == ("nyc", "u1") and ev.hlc == HLC(10, 1)


def test_delete_and_resolved():
    ev = parse_message('{"after": null, "key": [42], "updated": "5.0000000000"}', table="events")
    assert ev.is_delete
    r = parse_message('{"resolved": "99.0000000000"}', table="events")
    assert isinstance(r, ResolvedEvent) and r.hlc == HLC(99, 0)


@pytest.mark.parametrize("bad", [
    "not json", "[]", '{"after": {}, "updated": "1.0"}',  # no key
    '{"after": {}, "key": [1]}',  # no timestamp
])
def test_errors(bad):
    with pytest.raises(EnvelopeError):
        parse_message(bad, table="t")
