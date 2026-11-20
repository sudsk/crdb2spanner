-- 09 load generated while the streaming apply job is killed and restarted
USE torture;
INSERT INTO events (kind, attempts, priority, score, ratio, payload)
  SELECT 'stream', i % 10, 2, i::FLOAT8, 0.25, decode('aa', 'hex') FROM generate_series(1, 5000) AS g(i);
UPDATE type_zoo SET emoji = 'stream ' || id::STRING WHERE id BETWEEN 300 AND 700;
DELETE FROM type_zoo WHERE id BETWEEN 701 AND 750;
