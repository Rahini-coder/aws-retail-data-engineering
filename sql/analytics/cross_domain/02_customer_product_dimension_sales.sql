-- Join Silver order facts to master dimensions only; no Gold-to-Gold fact join.
-- Verify Silver and source master database names and catalog table names.
-- Master uniqueness is a prerequisite (enforced by the corresponding Gold jobs).
SELECT coalesce(c.country, 'UNKNOWN') AS customer_country,
       coalesce(p.category, 'UNKNOWN') AS product_category,
       count(DISTINCT o.order_id) AS completed_orders,
       sum(o.order_amount) AS completed_net_sales
FROM "<silver_database>"."silver_orders" o
LEFT JOIN "<source_database>"."<customers_table>" c
  ON trim(o.customer_id) = trim(c.customer_id)
LEFT JOIN "<source_database>"."<products_table>" p
  ON trim(o.product_id) = trim(p.product_id)
WHERE o.order_status = 'Completed'
GROUP BY 1, 2 ORDER BY completed_net_sales DESC;
