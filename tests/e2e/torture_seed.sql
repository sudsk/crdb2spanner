-- Seed data for the torture schema (bulk-load phase). Idempotent only after torture_schema.sql.
USE torture;

INSERT INTO customers (email, full_name, name_ci, country, profile, tags)
SELECT 'user' || i || '@example.com',
       'User ' || i,
       ('Name ' || i) COLLATE en_u_ks_level2,
       (ARRAY['GB', 'US', 'IN', 'DE'])[1 + i % 4],
       jsonb_build_object('tier', i % 3, 'prefs', jsonb_build_object('news', i % 2 = 0),
                          'hist', jsonb_build_array(i, i * 2)),
       ARRAY['t' || (i % 5)::STRING, 'all']
FROM generate_series(1, 2000) AS g(i);

INSERT INTO customers (email, full_name, name_ci, country, profile, tags) VALUES
  ('edge-unicode@example.com', '👍 café ñ 中文 Ω', 'ÉCOLE' COLLATE en_u_ks_level2, 'FR', '{"k": "☃"}', ARRAY['ünï', NULL, '']),
  ('edge-nulls@example.com', NULL, NULL, NULL, NULL, NULL),
  ('edge-empty@example.com', '', '' COLLATE en_u_ks_level2, '  ', '{}', ARRAY[]::STRING[]);

INSERT INTO "Order" (customer_id, order_no, status, amount, placed_at)
SELECT c.id, n,
       (ARRAY['new', 'paid', 'shipped', 'cancelled'])[1 + n % 4]::order_status,
       (c.rn * 13.3713 + n)::DECIMAL(18,4),
       '2026-01-01 00:00:00'::TIMESTAMP + (c.rn || ' hours')::INTERVAL
FROM (SELECT id, row_number() OVER (ORDER BY email) AS rn FROM customers) AS c,
     generate_series(1, 3) AS g(n)
WHERE c.rn <= 1500;

INSERT INTO order_items (customer_id, order_no, line_no, sku, qty)
SELECT customer_id, order_no, l, 'SKU-' || (order_no * l)::STRING, l
FROM "Order", generate_series(1, 2) AS g(l);

INSERT INTO events (kind, attempts, priority, score, ratio, payload)
SELECT 'k' || (i % 7)::STRING, i % 100, (i % 5)::INT2, i * 1.5, (i / 7.0)::FLOAT4,
       decode(lpad(to_hex(i), 8, '0'), 'hex')
FROM generate_series(1, 5000) AS g(i);

INSERT INTO events (kind, attempts, priority, score, ratio, payload) VALUES
  ('edge-nan',  2147483647, 32767,  'NaN'::FLOAT8,       'NaN'::FLOAT4,       decode('00ff00', 'hex')),
  ('edge-inf', -2147483648, -32768, 'Infinity'::FLOAT8,  '-Infinity'::FLOAT4, ''::BYTES),
  ('edge-ninf', 0,          0,      '-Infinity'::FLOAT8, 1.17549435e-38,      NULL);

INSERT INTO audit_log (actor)
SELECT 'actor' || (i % 10)::STRING FROM generate_series(1, 3000) AS g(i);

INSERT INTO readings (device_id, ts, t, ttz, dur, d)
SELECT i % 50,
       '2026-01-01 00:00:00+00'::TIMESTAMPTZ + (i || ' minutes')::INTERVAL,
       '00:00:00'::TIME + (i || ' seconds')::INTERVAL,
       '10:00:00+05:30'::TIMETZ,
       (i || ' days 3 hours 4 minutes 5.123456 seconds')::INTERVAL,
       '2026-01-01'::DATE + i
FROM generate_series(1, 4000) AS g(i);

INSERT INTO readings (device_id, ts, t, ttz, dur, d) VALUES
  (999, '0001-01-01 00:00:00+00', '00:00:00', '00:00:00-12:00', '-1 year -2 days', '0001-01-01'),
  (999, '9999-12-31 23:59:59.999999+00', '23:59:59.999999', '23:59:59+14:00', '1000 years 11 months 29 days 23:59:59.999999', '9999-12-31');

INSERT INTO type_zoo (id, big_dec, ip, bits, vbits, arr_int, arr_text, doc, bin, big_text, emoji) VALUES
  (1, 12345678901234567890.123456789012345, '192.168.0.1', B'10101010', B'101',
      ARRAY[1, NULL, 3], ARRAY['a', NULL, ''], '{"a": {"b": {"c": [1, 2, {"d": null}]}}}',
      decode('000102', 'hex'), 'plain', '👍 café ñ 中文'),
  (2, -0.0000000001, '::1', B'00000000', B'', ARRAY[]::INT8[], ARRAY[]::STRING[], '[]', ''::BYTES, '', ''),
  (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (4, 'NaN', '10.0.0.0/8', B'11111111', B'1', ARRAY[-9223372036854775808, 9223372036854775807],
      ARRAY['x'], '"just a string"', decode('ff', 'hex'), repeat('x', 200000), '🏳️‍🌈'),
  (5, 1e-30, '2001:db8::/32', B'00000001', B'0101010101', ARRAY[0], ARRAY['quote''s', 'back\slash'],
      '{"num": 1.0000000000000001, "big": 12345678901234567890}', decode('00', 'hex'), 'tab	and
newline', 'zero-width​space'),
  (-9223372036854775808, 0, NULL, NULL, NULL, NULL, NULL, 'null', NULL, 'min key', NULL),
  (9223372036854775807, 0, NULL, NULL, NULL, NULL, NULL, 'true', NULL, 'max key', NULL);

INSERT INTO type_zoo (id, big_dec, ip, arr_int, doc, bin, emoji)
SELECT i, (i * 1.000001)::DECIMAL, ('10.0.' || (i % 255)::STRING || '.1')::INET, ARRAY[i, i + 1],
       jsonb_build_object('i', i), decode(lpad(to_hex(i), 6, '0'), 'hex'), 'row ' || i::STRING
FROM generate_series(100, 1099) AS g(i);

INSERT INTO "user" (id, display)
SELECT 'u-' || i::STRING, 'User ' || i::STRING FROM generate_series(1, 500) AS g(i);

-- probes
INSERT INTO wide_families (id, a, b, c)
SELECT i, 'a' || i::STRING, 'b' || i::STRING, i FROM generate_series(1, 200) AS g(i);

INSERT INTO places (id, name, loc, shape)
SELECT i, 'place ' || i::STRING,
       ST_MakePoint(-0.1 + i * 0.001, 51.5)::GEOGRAPHY,
       'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'::GEOMETRY
FROM generate_series(1, 100) AS g(i);
