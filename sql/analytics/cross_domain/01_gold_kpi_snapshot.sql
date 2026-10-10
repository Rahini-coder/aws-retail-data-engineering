-- Independent scalar totals; do not join Gold rows across grains.
-- Run after a known consistent Gold refresh.
SELECT
 (SELECT sum(net_sales) FROM "customer_intelligence"."gold_daily_sales") AS daily_sales_net,
 (SELECT sum(lifetime_net_sales) FROM "customer_intelligence"."gold_customer_360") AS customer_net,
 (SELECT sum(net_sales) FROM "customer_intelligence"."gold_product_performance") AS product_net,
 (SELECT sum(total_orders) FROM "customer_intelligence"."gold_daily_sales") AS daily_order_count,
 (SELECT sum(lifetime_orders) FROM "customer_intelligence"."gold_customer_360") AS customer_order_count,
 (SELECT sum(lifetime_orders) FROM "customer_intelligence"."gold_product_performance") AS product_order_count;
