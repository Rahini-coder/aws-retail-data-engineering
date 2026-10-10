SELECT product_id, product_name, category, subcategory,
       lifetime_orders, unique_customers, units_sold,
       gross_sales, discount_amount, net_sales, average_selling_price,
       estimated_cogs, estimated_gross_margin, estimated_gross_margin_pct
FROM "customer_intelligence"."gold_product_performance"
WHERE product_sales_status = 'SOLD'
ORDER BY net_sales DESC, product_id LIMIT 20;
