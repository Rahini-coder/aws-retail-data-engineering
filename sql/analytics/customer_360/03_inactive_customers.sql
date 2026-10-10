-- 90-day threshold is an illustrative analytics parameter, not a V1 ETL rule.
-- days_since_last_order is relative to the Gold refresh date; verify last_refreshed_at.
SELECT customer_id, last_order_date, days_since_last_order,
       lifetime_orders, lifetime_net_sales, last_refreshed_at
FROM "<gold_database>"."gold_customer_360"
WHERE customer_purchase_status = 'PURCHASED'
  AND days_since_last_order >= 90
ORDER BY days_since_last_order DESC, lifetime_net_sales DESC LIMIT 100;
