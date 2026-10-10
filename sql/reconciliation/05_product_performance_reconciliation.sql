-- Mirrors gold_product_performance_v1.py Completed-only financial reconciliation.
WITH silver AS (
  SELECT COUNT(DISTINCT order_id) AS orders,
         COALESCE(SUM(CAST(order_amount AS DECIMAL(38,2))), CAST(0 AS DECIMAL(38,2))) AS net_sales
  FROM "customer_intelligence"."silver_orders" WHERE order_status = 'Completed'
), gold AS (
  SELECT COALESCE(SUM(lifetime_orders), 0) AS orders,
         COALESCE(SUM(net_sales), CAST(0 AS DECIMAL(38,2))) AS net_sales,
         COUNT(*) AS products, COUNT(DISTINCT product_id) AS distinct_products
  FROM "customer_intelligence"."gold_product_performance"
)
SELECT silver.orders AS silver_orders, gold.orders AS gold_orders,
       silver.orders - gold.orders AS order_difference,
       silver.net_sales - gold.net_sales AS net_sales_difference,
       gold.products, gold.distinct_products,
       gold.products - gold.distinct_products AS duplicate_or_null_product_ids
FROM silver CROSS JOIN gold;

-- Estimated margin is based on current product-master cost, not historical COGS.
