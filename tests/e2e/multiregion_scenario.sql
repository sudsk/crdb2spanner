-- CDC on multi-region tables, including moving rows between regions (crdb_region is part of
-- the physical key, so a region move changes the stored key but not the logical id)
USE mr;
UPDATE accounts SET balance = balance + 10 WHERE name LIKE 'acct 1%';
UPDATE accounts SET crdb_region = 'asia-south1' WHERE crdb_region = 'europe-west2' AND name LIKE 'acct 2%';
DELETE FROM accounts WHERE name LIKE 'acct 3%';
INSERT INTO accounts (name, balance, crdb_region) VALUES ('new-eu', 1, 'europe-west2'), ('new-us', 2, 'us-east1');
UPSERT INTO fx_rates VALUES ('USD', 1.30), ('JPY', 190.5);
DELETE FROM audit WHERE id % 7 = 0;
