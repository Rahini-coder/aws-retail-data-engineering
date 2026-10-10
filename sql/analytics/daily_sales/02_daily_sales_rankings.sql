-- Top 20 dates by net sales; one row per order_date in Gold.
SELECT order_date, total_orders, unique_customers, units_sold,
       gross_sales, discount_amount, net_sales, average_order_value
FROM "<gold_database>"."gold_daily_sales"
ORDER BY net_sales DESC, order_date DESC LIMIT 20;
