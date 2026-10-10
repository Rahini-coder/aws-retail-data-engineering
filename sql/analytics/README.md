# Business Analytics SQL — AWS Data Engineering Project 


## Query catalog

| File | Question answered |
|---|---|
| [`daily_sales/01_monthly_revenue_trend.sql`](daily_sales/01_monthly_revenue_trend.sql) | How are monthly net sales and order volume changing? |
| [`daily_sales/02_daily_sales_rankings.sql`](daily_sales/02_daily_sales_rankings.sql) | Which dates generated the most completed-order net sales? |
| [`daily_sales/03_weekly_sales.sql`](daily_sales/03_weekly_sales.sql) | What are weekly sales and average order values? |
| [`customer_360/01_top_customers.sql`](customer_360/01_top_customers.sql) | Who are the top customers by completed-order lifetime spend? |
| [`customer_360/02_customer_segments.sql`](customer_360/02_customer_segments.sql) | How many customers fall into simple purchase-frequency bands? |
| [`customer_360/03_inactive_customers.sql`](customer_360/03_inactive_customers.sql) | Which previously purchasing customers had 90+ days since last order at Gold refresh? |
| [`customer_360/04_acquisition_channel_summary.sql`](customer_360/04_acquisition_channel_summary.sql) | How do completed-order sales vary by acquisition channel? |
| [`product_performance/01_top_products.sql`](product_performance/01_top_products.sql) | Which products generate the highest completed-order sales? |
| [`product_performance/02_category_performance.sql`](product_performance/02_category_performance.sql) | What are category sales and weighted estimated margins? |
| [`product_performance/03_never_sold_products.sql`](product_performance/03_never_sold_products.sql) | Which catalog products have no completed sales? |
| [`product_performance/04_margin_watchlist.sql`](product_performance/04_margin_watchlist.sql) | Which sold products have negative estimated gross margin? |
| [`cross_domain/01_gold_kpi_snapshot.sql`](cross_domain/01_gold_kpi_snapshot.sql) | What are the independently aggregated totals across Gold outputs? |
| [`cross_domain/02_customer_product_dimension_sales.sql`](cross_domain/02_customer_product_dimension_sales.sql) | How do completed-order sales break down by customer country and product category? |

## Verified script-derived business definitions

- All three Gold jobs filter Silver to `order_status = 'Completed'`.
- Gross sales = `quantity * unit_price`; net sales = `order_amount`; discount = gross minus net.
- Daily Sales is at **order date** grain. Customer 360 is at **customer** grain, preserving master customers without completed orders. Product Performance is at **product** grain, preserving master products without completed orders.
- Product `estimated_cogs = units_sold * unit_cost` uses **current product master cost**; estimated margin is not historical cost accounting.
- `monthly_subscription_revenue` is a customer-master attribute, not part of completed-order sales; do not add it to transaction net sales.
- Gold order counts are `countDistinct(order_id)` within each grouping. Summing counts across groups assumes an order does not span multiple groups; this is a V1 reconciliation assumption, not a general line-item model.




