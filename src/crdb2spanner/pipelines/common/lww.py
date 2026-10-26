"""Last-writer-wins filter: a stateful DoFn keyed by (table, pk).

Drops any event whose HLC is <= the highest HLC already seen for that key. This makes
duplicate, replayed and out-of-order changefeed messages harmless *within a running job*.
Cross-restart correctness comes from the shadow-table check in strict apply mode.
"""

from __future__ import annotations

import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.metrics import Metrics
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec

from .envelope import HLC


class LatestWinsDoFn(beam.DoFn):
    MAX_HLC = ReadModifyWriteStateSpec("max_hlc", StrUtf8Coder())

    def __init__(self):
        self.passed = Metrics.counter("cdc", "lww_passed")
        self.dropped = Metrics.counter("cdc", "lww_stale_dropped")

    def process(self, element, max_hlc=beam.DoFn.StateParam(MAX_HLC)):
        _, event = element
        cur = max_hlc.read()
        if cur is not None and HLC.parse(cur) >= event.hlc:
            self.dropped.inc()
            return
        max_hlc.write(str(event.hlc))
        self.passed.inc()
        yield event
    # TODO(M3): state-expiry timer so per-key state does not grow unbounded on huge tables.
