-- Calendar weeks start Monday in Athena/Trino date_trunc('week').
SELECT date_trunc('week', order_date) AS week_start,
       sum(total_orders) AS orders, sum(units_sold) AS units_sold,
       sum(net_sales) AS net_sales,
       round(sum(net_sales) / nullif(sum(total_orders), 0), 2) AS average_order_value
FROM "customer_intelligence"."gold_daily_sales"
GROUP BY 1 ORDER BY week_start;
