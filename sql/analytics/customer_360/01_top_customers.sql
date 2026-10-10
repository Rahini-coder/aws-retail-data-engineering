-- Lifetime completed-order value; master includes customers with no purchases.
SELECT customer_id, country, state, city, acquisition_channel,
       lifetime_orders, lifetime_units, lifetime_net_sales,
       average_order_value, last_order_date
FROM "customer_intelligence"."gold_customer_360"
WHERE customer_purchase_status = 'PURCHASED'
ORDER BY lifetime_net_sales DESC, customer_id LIMIT 20;
