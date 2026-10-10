-- Completed orders only; Gold daily sales is already Completed-filtered.
-- Replace <gold_database> with the verified Glue database.
WITH monthly AS (
 SELECT date_trunc('month', order_date) AS sales_month,
        sum(total_orders) AS orders,
        sum(units_sold) AS units_sold,
        sum(net_sales) AS net_sales,
        sum(gross_sales) AS gross_sales,
        sum(discount_amount) AS discount_amount
 FROM "customer_intelligence"."gold_daily_sales"
 GROUP BY 1
)
SELECT sales_month, orders, units_sold, gross_sales, discount_amount, net_sales,
       round(net_sales / nullif(orders, 0), 2) AS weighted_average_order_value,
       lag(net_sales) OVER (ORDER BY sales_month) AS previous_month_sales,
       round(100.0 * (net_sales - lag(net_sales) OVER (ORDER BY sales_month)) /
         nullif(lag(net_sales) OVER (ORDER BY sales_month), 0), 2) AS previous_observed_month_change_pct
FROM monthly ORDER BY sales_month;
