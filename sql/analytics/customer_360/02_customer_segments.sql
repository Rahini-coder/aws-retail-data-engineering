-- Descriptive lifetime-order frequency segments; not predictive CLV or cohort retention.
WITH classified AS (
 SELECT customer_id, lifetime_orders, lifetime_net_sales,
   CASE WHEN lifetime_orders = 0 THEN 'NEVER_PURCHASED'
        WHEN lifetime_orders = 1 THEN 'ONE_ORDER'
        WHEN lifetime_orders BETWEEN 2 AND 4 THEN 'TWO_TO_FOUR_ORDERS'
        ELSE 'FIVE_PLUS_ORDERS' END AS purchase_segment
 FROM "<gold_database>"."gold_customer_360"
)
SELECT purchase_segment, count(*) AS customers,
       sum(lifetime_orders) AS completed_orders,
       sum(lifetime_net_sales) AS lifetime_net_sales,
       round(avg(lifetime_net_sales), 2) AS average_customer_sales
FROM classified GROUP BY 1 ORDER BY 1;
