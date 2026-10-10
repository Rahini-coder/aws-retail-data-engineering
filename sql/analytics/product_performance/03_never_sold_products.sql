-- Product master left join retains products with zero completed orders.
SELECT product_id, product_name, category, subcategory,
       catalog_unit_price, unit_cost, launch_date
FROM "customer_intelligence"."gold_product_performance"
WHERE product_sales_status = 'NEVER_SOLD'
ORDER BY category, product_name, product_id;
