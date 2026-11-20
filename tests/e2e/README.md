# End-to-end "torture" test

This is a purpose-built CockroachDB database where every table targets a group of CockroachDB → Spanner (PostgreSQL dialect) incompatibilities. A set of CDC scenarios then runs on top of it, all through the real tool: CockroachDB in Docker → `EXPORT` / changefeeds → Beam pipelines → Spanner emulator.

```bash
pip install -e ".[dev]"
./scripts/e2e_torture.sh          # or: make e2e-torture
```

It writes **`.local/e2e/torture-results.txt`** (one line per check) and **`.local/e2e/torture.log`** (full output). Every check is recorded and the run continues past failures; only setup steps abort.

## Tables and what they test

| Table | Incompatibilities covered | Expected handling |
|---|---|---|
| `customers` | UUID PK (the case Google's JDBC template fails on); UNIQUE; collated string; JSONB + inverted index; `STRING[]`; STORING index; `CHAR(2)`; unicode, empty and NULL edge rows | UUID → `varchar(36)` with no source change. Collation is dropped and flagged. The inverted index is skipped and flagged. STORING becomes `INCLUDE` |
| `"Order"` | Reserved word and mixed-case name; composite PK with DESC; ENUM; `DECIMAL(18,4)`; STORED and VIRTUAL computed columns; partial index; FK with ON DELETE CASCADE; `TIMESTAMP` (no time zone) | Quoted identifier. ENUM → `varchar`. STORED computed columns are copied as plain values. **VIRTUAL computed columns are not migrated** and are flagged. The partial index is created without its predicate. FKs are applied after cutover |
| `order_items` | Composite FK chain with cascade; CHECK constraint; INT4 | Cascaded deletes arrive as CDC deletes |
| `events` | **No PK (hidden `rowid`)**; INT4/INT2; FLOAT NaN and ±Infinity; BYTES with `\x00`; row TTL | `rowid` is made explicit and bit-reversed. Ints are widened to `bigint`. TTL is flagged |
| `audit_log` | Visible `unique_rowid()` PK (monotonic); `SERIAL`; hash-sharded secondary index | Hotspot risk flagged. The shard column is dropped from the index |
| `readings` | **Hash-sharded PK**; TIME, TIMETZ, INTERVAL; DATE and TIMESTAMPTZ at 0001-01-01 and 9999-12-31 | Time types are stored as ISO text. Watch for key-shape mismatches caused by the hidden shard column in the changefeed key |
| `type_zoo` | Unbounded DECIMAL including NaN; INET; BIT and VARBIT; arrays with NULLs and empty arrays; nested and scalar JSONB; BYTES; 200–300 KB text; emoji and zero-width characters; INT8 min/max keys | Transforms and canonical hashing in validation |
| `"user"` | Reserved-word name; STRING PK; PK renames | — |
| **Probes:** `wide_families` | Multiple column families | **Known gap:** changefeeds need `split_column_families`, which CDC apply doesn't support yet |
| **Probes:** `places` | GEOGRAPHY and GEOMETRY | **Unknown:** whether `EXPORT` to Parquet and changefeeds handle these. Mapped to text |

Probe tables live in a separate job (`job.torture-probes.yaml`). Their steps are recorded as `PROBE-OK` or `PROBE-GAP` and never fail the run.

## Scenarios (in run order)

| # | Scenario | What it proves |
|---|---|---|
| 1 | Snapshot `AS OF SYSTEM TIME T0`, bulk load, validate at T0 | The initial load is exact and consistent |
| 2 | Changefeeds from `cursor = T0` | No gap or overlap between the load and CDC |
| 01 | Mixed DML across all tables | Basic CDC apply, including STORED computed columns changing |
| 02 | Primary-key updates (int, string, UUID) | The delete-old plus insert-new pair converges |
| 03 | Delete and re-insert the same key; UPSERT nulling columns; insert-then-delete | Newest-version-wins per key, and deletes aren't brought back by stale rows |
| 04 | 200 sequential updates to one row | Ordering and newest-version-wins under a hot key |
| 05 | One 20k-row multi-table transaction | Large batches; transaction boundaries aren't needed for the final state |
| 06 | FK cascade delete across three tables | Cascaded deletes arrive and are applied |
| 07 | Edge values through CDC (NaN, empty and NULL arrays, 300 KB text, year 9999) | JSON-envelope encodings match the Parquet encodings |
| 6 | Replay every CDC file again | Duplicates are harmless |
| 7 | Pause changefeeds, write, then resume | Nothing is lost across a pause |
| 8 | Poison messages in the CDC stream | They land in the DLQ and the pipeline carries on |
| 9 | Streaming apply (`--follow`, strict mode) killed with -9 mid-load, then restarted | Shadow tables give convergence after a crash |
| 10 | `ALTER TABLE` on the source during replication | That table's changefeed stops cleanly while the others keep running |
| 11 | Apply secondary indexes, then FKs, on the target | The deferred DDL is valid Spanner PostgreSQL DDL |

**Pass criteria:** every `validate` is green, the DLQ contains only the injected poison messages, and the source schema is never altered by the tool. (Scenario 10's `ALTER TABLE` is a deliberate test action.)

## Notes

- Everything runs against the **Spanner emulator**. Repeat on a real 100-PU PostgreSQL-dialect instance (`examples/job.gcp.yaml`) before trusting performance or IAM behaviour.
- Single-node CockroachDB can't test `REGIONAL BY ROW`, which needs a multi-region cluster. The converter covers it in unit tests only.
- If `changefeed create` fails with a licence error, see `docs/test-environment.md` §5.
