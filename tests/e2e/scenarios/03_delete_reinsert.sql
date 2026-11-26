-- 03 delete then re-insert the same key; UPSERT resetting columns to NULL
USE torture;
DELETE FROM type_zoo WHERE id = 130;
INSERT INTO type_zoo (id, emoji) VALUES (130, 'reborn');
DELETE FROM "user" WHERE id = 'u-10';
INSERT INTO "user" (id, display) VALUES ('u-10', 'Reborn');
UPSERT INTO type_zoo (id, emoji) VALUES (131, 'upserted - other columns now NULL');
DELETE FROM type_zoo WHERE id = 132;
INSERT INTO type_zoo (id, emoji) VALUES (132, 'second life');
DELETE FROM type_zoo WHERE id = 132;   -- final state: absent
