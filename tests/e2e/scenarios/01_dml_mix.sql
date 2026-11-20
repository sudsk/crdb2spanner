-- 01 mixed inserts / updates / deletes across all main tables
USE torture;
UPDATE customers SET full_name = full_name || ' (upd)', profile = profile || '{"updated": true}' WHERE country = 'GB';
INSERT INTO customers (email, full_name, country, tags)
  SELECT 'new' || i::STRING || '@example.com', 'New ' || i::STRING, 'FR', ARRAY['new'] FROM generate_series(1, 200) AS g(i);
DELETE FROM events WHERE attempts < 5;
UPDATE "Order" SET status = 'shipped' WHERE status = 'paid';
UPDATE "Order" SET amount = amount + 1 WHERE order_no = 2;   -- STORED computed column tax changes too
INSERT INTO audit_log (actor) SELECT 'cdc' || i::STRING FROM generate_series(1, 300) AS g(i);
UPDATE readings SET dur = dur + '1 hour'::INTERVAL, t = t + '1 second'::INTERVAL WHERE device_id = 7;
UPDATE type_zoo SET arr_int = array_append(arr_int, 42), doc = doc || '{"touched": 1}' WHERE id BETWEEN 100 AND 199;
INSERT INTO "user" VALUES ('u-new-1', 'New user');
