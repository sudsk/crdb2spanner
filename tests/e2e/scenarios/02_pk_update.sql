-- 02 primary-key updates: arrive as delete(old key) + insert(new key)
USE torture;
UPDATE type_zoo SET id = id + 1000000 WHERE id BETWEEN 110 AND 120;
UPDATE "user" SET id = id || '-renamed' WHERE id IN ('u-1', 'u-2');
UPDATE customers SET id = gen_random_uuid() WHERE email LIKE 'new1%@example.com';  -- no children
