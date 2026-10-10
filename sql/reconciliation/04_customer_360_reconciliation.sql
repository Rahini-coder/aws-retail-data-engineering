-- Mirrors customer_360_v2.py Completed-only order/net-sales reconciliation.
WITH silver AS (
  SELECT COUNT(DISTINCT order_id) AS orders,
         COALESCE(SUM(CAST(order_amount AS DECIMAL(38,2))), CAST(0 AS DECIMAL(38,2))) AS net_sales
  FROM "<silver_database>"."silver_orders" WHERE order_status = 'Completed'
), gold AS (
  SELECT COALESCE(SUM(lifetime_orders), 0) AS orders,
         COALESCE(SUM(lifetime_net_sales), CAST(0 AS DECIMAL(38,2))) AS net_sales,
         COUNT(*) AS customers, COUNT(DISTINCT customer_id) AS distinct_customers
  FROM "<gold_database>"."gold_customer_360"
)
SELECT silver.orders AS silver_orders, gold.orders AS gold_orders,
       silver.orders - gold.orders AS order_difference,
       silver.net_sales - gold.net_sales AS net_sales_difference,
       gold.customers, gold.distinct_customers,
       gold.customers - gold.distinct_customers AS duplicate_or_null_customer_ids
FROM silver CROSS JOIN gold;

-- To verify preservation of every customer master record, compare Gold count
-- to the crawler-created customer master table after confirming its actual name.
