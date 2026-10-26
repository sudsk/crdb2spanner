# Type mapping: CockroachDB → Spanner (PostgreSQL dialect)

Generated from `src/crdb2spanner/schema/types.py` (the code is the source of truth). The *transform* is applied identically in bulk load, CDC apply and validation.

| CockroachDB | Spanner PG | Transform | Severity | Note |
|---|---|---|---|---|
| `INT8` | `bigint` | `int` |  |  |
| `INT4` | `bigint` | `int` | medium | INT4 widened to bigint (app impact) |
| `INT2` | `bigint` | `int` | medium | INT2 widened to bigint (app impact) |
| `FLOAT8` | `double precision` | `float` |  |  |
| `FLOAT4` | `real` | `float` |  |  |
| `DECIMAL` | `numeric` | `decimal` |  |  |
| `DECIMAL(10,2)` | `numeric` | `decimal` | info | precision/scale 10,2 not enforced |
| `BOOL` | `boolean` | `bool` |  |  |
| `STRING` | `varchar` | `to_text` |  |  |
| `STRING(50)` | `varchar(50)` | `to_text` |  |  |
| `VARCHAR(255)` | `varchar(255)` | `to_text` |  |  |
| `CHAR(3)` | `varchar(3)` | `to_text` | info | CHAR padding semantics not kept |
| `BYTES` | `bytea` | `bytes` |  |  |
| `DATE` | `date` | `date` |  |  |
| `TIMESTAMPTZ` | `timestamptz` | `timestamp` |  |  |
| `TIMESTAMP` | `timestamptz` | `timestamp_naive_utc` | medium | TIMESTAMP (no tz) stored as timestamptz assuming UTC |
| `TIME` | `varchar(32)` | `to_text` | medium | TIME stored as ISO text |
| `TIMETZ` | `varchar(40)` | `to_text` | medium | TIMETZ stored as ISO text |
| `INTERVAL` | `varchar(64)` | `to_text` | medium | INTERVAL stored as text |
| `UUID` | `varchar(36)` | `uuid` |  |  |
| `JSONB` | `jsonb` | `json` |  |  |
| `INET` | `varchar(64)` | `to_text` | medium | INET stored as text |
| `BIT` | `varchar` | `to_text` | medium | BIT stored as text |
| `VARBIT` | `varchar` | `to_text` | medium | VARBIT stored as text |
| `GEOMETRY` | `varchar` | `to_text` | high | Geospatial stored as WKT/EWKB text |
| `GEOGRAPHY` | `varchar` | `to_text` | high | Geospatial stored as WKT/EWKB text |
| `TSVECTOR` | `varchar` | `to_text` | high | Use Spanner full-text search instead |
| `OID` | `bigint` | `int` | medium |  |
| `STRING[]` | `varchar[]` | `array:to_text` |  |  |
| `INT8[]` | `bigint[]` | `array:int` |  |  |
| `UUID[]` | `varchar(36)[]` | `array:uuid` |  |  |
| `my_enum` | `varchar(255)` | `to_text` | info | ENUM my_enum stored as text |

Special cases handled by the converter:

- **Hidden `rowid` PK** (`unique_rowid()`): made explicit as `rowid bigint` and **bit-reversed** (`bit_reverse`), so monotonically increasing ids spread across Spanner's key space. The mapping is reversible and stays unique.
- **Visible `unique_rowid()` / `nextval()` PKs** are flagged *high*. Choose a bit-reversed Spanner sequence or UUID per table (app impact).
- **`UUID`** maps to `varchar(36)` by default. Set `uuid_native: true` in the job config to use Spanner's `uuid` type, if your Spanner version supports it in the PG dialect (decision D8).
- **Defaults**: `gen_random_uuid()` becomes `spanner.generate_uuid()`, `now()` becomes `CURRENT_TIMESTAMP`, and literal constants are kept. Other defaults are reported and not migrated.
- **Computed columns** are migrated as plain columns (values come from the source) and flagged.
- **Hash-shard columns** (`crdb_internal_*_shard_*`) are dropped from secondary indexes.
- **Inverted indexes** are skipped and flagged. **Partial indexes** are created without their predicate and flagged.
