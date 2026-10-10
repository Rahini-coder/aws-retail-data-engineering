-- Compare published current-state Gold net-sales totals.
-- All three are expected to match after a consistent successful refresh.
WITH daily AS (
  SELECT COALESCE(SUM(net_sales), CAST(0 AS DECIMAL(38,2))) AS amount
  FROM "<gold_database>"."gold_daily_sales"
), customer AS (
  SELECT COALESCE(SUM(lifetime_net_sales), CAST(0 AS DECIMAL(38,2))) AS amount
  FROM "<gold_database>"."gold_customer_360"
), product AS (
  SELECT COALESCE(SUM(net_sales), CAST(0 AS DECIMAL(38,2))) AS amount
  FROM "<gold_database>"."gold_product_performance"
)
SELECT daily.amount AS daily_sales_net,
       customer.amount AS customer_360_net,
       product.amount AS product_performance_net,
       daily.amount - customer.amount AS daily_vs_customer_difference,
       daily.amount - product.amount AS daily_vs_product_difference
FROM daily CROSS JOIN customer CROSS JOIN product;
