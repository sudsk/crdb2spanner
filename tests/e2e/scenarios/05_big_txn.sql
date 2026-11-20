-- 05 one large multi-table transaction
USE torture;
BEGIN;
INSERT INTO events (kind, attempts, priority, score, ratio, payload)
  SELECT 'bulk', i % 10, 1, i::FLOAT8, 0.5, NULL FROM generate_series(1, 20000) AS g(i);
UPDATE events SET priority = 4 WHERE kind = 'k1';
DELETE FROM audit_log WHERE actor = 'actor3';
UPDATE readings SET d = d + 1 WHERE device_id < 10;
COMMIT;
