-- Multi-region source: REGIONAL BY ROW, GLOBAL and REGIONAL BY TABLE tables.
-- Runs against the "multiregion" compose profile (3 nodes in 3 regions).
DROP DATABASE IF EXISTS mr CASCADE;
CREATE DATABASE mr PRIMARY REGION "europe-west2" REGIONS "us-east1", "asia-south1";
USE mr;

-- each row homed in a region via the hidden crdb_region column (implicit key prefix)
CREATE TABLE accounts (
  id      UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  name    STRING NOT NULL,
  balance DECIMAL(18,2),
  INDEX accounts_name_idx (name)
) LOCALITY REGIONAL BY ROW;

-- low-latency reads everywhere
CREATE TABLE fx_rates (
  ccy  STRING PRIMARY KEY,
  rate DECIMAL NOT NULL
) LOCALITY GLOBAL;

-- whole table homed in one region
CREATE TABLE audit (
  id   INT8 NOT NULL DEFAULT unique_rowid() PRIMARY KEY,
  note STRING
) LOCALITY REGIONAL BY TABLE IN "us-east1";

INSERT INTO accounts (name, balance, crdb_region)
SELECT 'acct ' || i::STRING, (i * 1.5)::DECIMAL(18,2),
       (ARRAY['europe-west2', 'us-east1', 'asia-south1'])[1 + i % 3]::crdb_internal_region
FROM generate_series(1, 1500) AS g(i);

INSERT INTO fx_rates VALUES ('GBP', 1.0), ('USD', 1.27), ('INR', 105.1), ('EUR', 1.16);

INSERT INTO audit (note) SELECT 'note ' || i::STRING FROM generate_series(1, 300) AS g(i);
