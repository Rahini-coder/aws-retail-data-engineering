-- Completed-only reconciliation as implemented by gold_daily_sales_v1.py.
-- Run after a known successful Gold refresh and before a subsequent Silver update.
WITH silver AS (
  SELECT COUNT(DISTINCT order_id) AS orders,
         COALESCE(SUM(CAST(order_amount AS DECIMAL(38,2))), CAST(0 AS DECIMAL(38,2))) AS net_sales
  FROM "<silver_database>"."silver_orders"
  WHERE order_status = 'Completed'
), gold AS (
  SELECT COALESCE(SUM(total_orders), 0) AS orders,
         COALESCE(SUM(net_sales), CAST(0 AS DECIMAL(38,2))) AS net_sales
  FROM "<gold_database>"."gold_daily_sales"
)
SELECT silver.orders AS silver_orders, gold.orders AS gold_orders,
       silver.orders - gold.orders AS order_difference,
       silver.net_sales AS silver_net_sales, gold.net_sales AS gold_net_sales,
       silver.net_sales - gold.net_sales AS net_sales_difference
FROM silver CROSS JOIN gold;
