-- Margin is estimated using current master cost, not historical transaction cost.
SELECT coalesce(category, 'UNKNOWN') AS category,
       count(*) AS catalog_products,
       count_if(product_sales_status = 'SOLD') AS sold_products,
       sum(units_sold) AS units_sold,
       sum(net_sales) AS net_sales,
       sum(estimated_cogs) AS estimated_cogs,
       sum(estimated_gross_margin) AS estimated_gross_margin,
       round(100.0 * sum(estimated_gross_margin) / nullif(sum(net_sales), 0), 2)
          AS weighted_estimated_gross_margin_pct
FROM "<gold_database>"."gold_product_performance"
GROUP BY 1 ORDER BY net_sales DESC;
