-- 06 FK cascade deletes (customers -> "Order" -> order_items)
USE torture;
DELETE FROM customers WHERE country = 'DE' AND email < 'user5';
