-- Investigative watchlist only; estimated margin can be negative.
SELECT product_id, product_name, category, net_sales,
       estimated_cogs, estimated_gross_margin, estimated_gross_margin_pct
FROM "<gold_database>"."gold_product_performance"
WHERE product_sales_status = 'SOLD' AND estimated_gross_margin < 0
ORDER BY estimated_gross_margin ASC, product_id;
