-- 07 edge values arriving through CDC (not only through the bulk load)
USE torture;
INSERT INTO type_zoo (id, big_dec, ip, bits, vbits, arr_int, arr_text, doc, bin, big_text, emoji) VALUES
  (500, 'NaN', '255.255.255.255/32', B'00000000', B'', ARRAY[NULL]::INT8[], ARRAY[NULL, 'z'], '{"deep": [[[[1]]]]}',
   decode('00000000', 'hex'), repeat('y', 300000), '🧪 ∑ ¿');
UPDATE type_zoo SET arr_int = ARRAY[]::INT8[], doc = '[]' WHERE id = 1;
UPDATE type_zoo SET big_dec = -99999999999999999999.99999999999999999999 WHERE id = 2;
UPDATE events SET score = 'NaN'::FLOAT8 WHERE kind = 'edge-inf';
INSERT INTO readings (device_id, ts, t, ttz, dur, d)
  VALUES (998, '9999-12-31 00:00:00+00', '12:00:00', '12:00:00+00:00', '0 seconds', '9999-12-31');
UPDATE customers SET name_ci = 'école' COLLATE en_u_ks_level2 WHERE email = 'edge-unicode@example.com';
