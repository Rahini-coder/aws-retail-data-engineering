-- Subscription revenue is a master attribute; do NOT add it to completed-order sales.
SELECT coalesce(acquisition_channel, 'UNKNOWN') AS acquisition_channel,
       count(*) AS customers,
       count_if(customer_purchase_status = 'PURCHASED') AS purchasing_customers,
       sum(lifetime_orders) AS completed_orders,
       sum(lifetime_net_sales) AS completed_order_net_sales
FROM "<gold_database>"."gold_customer_360"
GROUP BY 1 ORDER BY completed_order_net_sales DESC;
