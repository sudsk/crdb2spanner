-- 08 writes made while the changefeeds are paused
USE torture;
INSERT INTO "user" (id, display) SELECT 'paused-' || i::STRING, 'while paused' FROM generate_series(1, 50) AS g(i);
UPDATE type_zoo SET emoji = 'changed while paused' WHERE id BETWEEN 200 AND 250;
DELETE FROM audit_log WHERE actor = 'actor4';
