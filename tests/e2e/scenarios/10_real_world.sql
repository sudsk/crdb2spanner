-- 10 real-world patterns through CDC: sequence-generated keys, natural-key updates on a keyless
--    table (needs the before image), renamed column, expression-indexed column
USE torture;
INSERT INTO job_execution (status) SELECT 'STARTED' FROM generate_series(1, 25);
UPDATE job_execution SET status = 'COMPLETED' WHERE status = 'STARTED' AND id % 2 = 0;
UPDATE job_execution_params SET value = 'changed' WHERE key_name = 'mode';
UPDATE job_execution_params SET key_name = 'input.path' WHERE key_name = 'input.file' AND job_execution_id % 5 = 0;
DELETE FROM job_execution_params WHERE key_name = 'run.id' AND job_execution_id % 7 = 0;
INSERT INTO job_execution_params (job_execution_id, key_name, value)
  SELECT id, 'retry', 'yes' FROM job_execution WHERE status = 'FAILED' LIMIT 20;
UPDATE messages SET sender_email = upper(sender_email), body = body || '{"seen": true}' WHERE _id LIKE 'm-1%';
DELETE FROM messages WHERE _id LIKE 'm-9%';
INSERT INTO messages (_id, body, sender_email) VALUES ('m-new', '{}', 'New@Example.com');
