-- crdb2spanner end-to-end "torture" schema: one table per group of CockroachDB -> Spanner
-- incompatibilities. Main job: every table except those in the PROBES section.
-- See tests/e2e/README.md for what each table is expected to exercise.

DROP DATABASE IF EXISTS torture CASCADE;
CREATE DATABASE torture;
USE torture;

CREATE TYPE order_status AS ENUM ('new', 'paid', 'shipped', 'cancelled');

-- T1  UUID primary key (the case Google's JDBC template fails on), UNIQUE, collated string,
--     JSONB + inverted index, STRING[] array, STORING index, CHAR(n)
CREATE TABLE customers (
  id         UUID        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  email      STRING      NOT NULL UNIQUE,
  full_name  STRING(200),
  name_ci    STRING COLLATE en_u_ks_level2,
  country    CHAR(2),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  profile    JSONB,
  tags       STRING[],
  INDEX customers_country_idx (country) STORING (full_name),
  INDEX customers_email_idx (email),            -- same key as the UNIQUE email index: skipped
  INVERTED INDEX customers_profile_idx (profile)
);

-- T2  reserved + mixed-case table name, composite PK with DESC, ENUM, DECIMAL(p,s),
--     STORED and VIRTUAL computed columns, partial index, FK with ON DELETE CASCADE,
--     TIMESTAMP without time zone
CREATE TABLE "Order" (
  customer_id  UUID          NOT NULL REFERENCES customers (id) ON DELETE CASCADE,
  order_no     INT8          NOT NULL,
  status       order_status  NOT NULL DEFAULT 'new',
  amount       DECIMAL(18,4) NOT NULL,
  tax          DECIMAL(18,4) AS (amount * 0.2) STORED,
  amount_cents INT8          AS ((amount * 100)::INT8) VIRTUAL,
  placed_at    TIMESTAMP     NOT NULL DEFAULT now(),
  PRIMARY KEY (customer_id, order_no DESC),
  INDEX order_open_idx (status) WHERE status != 'cancelled'
);

-- T3  grandchild: composite FK chain with cascade, CHECK constraint, INT4
CREATE TABLE order_items (
  customer_id UUID   NOT NULL,
  order_no    INT8   NOT NULL,
  line_no     INT4   NOT NULL,
  sku         STRING NOT NULL,
  qty         INT8   NOT NULL CHECK (qty > 0),
  PRIMARY KEY (customer_id, order_no, line_no),
  FOREIGN KEY (customer_id, order_no) REFERENCES "Order" (customer_id, order_no) ON DELETE CASCADE
);

-- T4  NO primary key (hidden rowid), INT4/INT2, FLOAT8/FLOAT4 (NaN, +/-Inf), BYTES, row TTL
CREATE TABLE events (
  kind     STRING NOT NULL,
  attempts INT4,
  priority INT2,
  score    FLOAT8,
  ratio    FLOAT4,
  payload  BYTES
) WITH (ttl_expire_after = '3650 days');

-- T5  visible unique_rowid() PK (monotonic), SERIAL, hash-sharded secondary index
CREATE TABLE audit_log (
  id    INT8        NOT NULL DEFAULT unique_rowid() PRIMARY KEY,
  seq   SERIAL,
  actor STRING,
  at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  INDEX audit_at_idx (at) USING HASH
);

-- T6  hash-sharded PRIMARY KEY, TIME, TIMETZ, INTERVAL, DATE extremes
CREATE TABLE readings (
  device_id INT8        NOT NULL,
  ts        TIMESTAMPTZ NOT NULL,
  t         TIME,
  ttz       TIMETZ,
  dur       INTERVAL,
  d         DATE,
  PRIMARY KEY (device_id, ts) USING HASH
);

-- T7  type zoo: unbounded DECIMAL (incl. NaN), INET, BIT, VARBIT, arrays with NULLs and
--     empty arrays, nested JSONB, BYTES with \x00, unicode/emoji, large text, INT8 min/max keys
CREATE TABLE type_zoo (
  id       INT8 PRIMARY KEY,
  big_dec  DECIMAL,
  ip       INET,
  bits     BIT(8),
  vbits    VARBIT,
  arr_int  INT8[],
  arr_text STRING[],
  doc      JSONB,
  bin      BYTES,
  big_text STRING,
  emoji    STRING
);

-- T8  reserved-word table name, STRING primary key, unique index identical to the PK,
--     FK to a UNIQUE non-key column of another table
CREATE TABLE "user" (
  id      STRING PRIMARY KEY,
  display STRING,
  email   STRING REFERENCES customers (email),
  UNIQUE INDEX user_id_uq (id)
);

-- T9  leading-underscore column name, expression index, column-based row TTL,
--     index name reused on another table (database-wide names in Spanner)
CREATE TABLE messages (
  _id               STRING      NOT NULL PRIMARY KEY,
  body              JSONB,
  sender_email      STRING,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  INDEX messages_sender_lower_idx ((lower(sender_email))),
  INDEX by_inserted (inserted_at)
) WITH (ttl_expiration_expression = 'inserted_at + INTERVAL ''90 days''');

-- T10 sequence-backed key (batch-framework style), standalone sequences (one INCREMENT 50), CHECK on a status,
--     duplicate index, index name clashing with T9
CREATE SEQUENCE job_execution_seq;
CREATE SEQUENCE hibernate_sequence;
CREATE SEQUENCE orders_seq INCREMENT 50;        -- ORM pooled allocation (allocationSize=50)
CREATE TABLE job_execution (
  id      INT8        NOT NULL DEFAULT nextval('job_execution_seq') PRIMARY KEY,
  status  STRING      NOT NULL DEFAULT 'STARTED' CHECK (status IN ('STARTED', 'COMPLETED', 'FAILED')),
  created TIMESTAMPTZ NOT NULL DEFAULT now(),
  INDEX by_inserted (created),
  INDEX job_status_idx (status),
  INDEX job_status_dup_idx (status)
);

-- T11 keyless table with a natural key (primary_key_overrides in the job config)
CREATE TABLE job_execution_params (
  job_execution_id INT8   NOT NULL,
  key_name         STRING NOT NULL,
  value            STRING
);

-- ===================================== PROBES =====================================
-- Expected to hit known gaps. Excluded from the main job; run separately so findings
-- are recorded without blocking the main flow.

-- P1  multiple column families (changefeed requires split_column_families)
CREATE TABLE wide_families (
  id INT8 PRIMARY KEY,
  a  STRING,
  b  STRING,
  c  INT8,
  FAMILY fa (id, a),
  FAMILY fb (b, c)
);

-- P2  geospatial types
CREATE TABLE places (
  id    INT8 PRIMARY KEY,
  name  STRING,
  loc   GEOGRAPHY,
  shape GEOMETRY
);
