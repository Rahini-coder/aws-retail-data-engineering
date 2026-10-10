# Athena Validation & Reconciliation — AWS Data Engineering 

## Scope and assumptions

- Silver is an Iceberg table, normally called `silver_orders`; incremental quarantine is an Iceberg table, normally called `orders_quarantine`. The scripts receive their database and table names through Glue arguments; 
- The three Gold tables are Iceberg current-state full-refresh outputs: `gold_daily_sales`, `gold_customer_360`, and `gold_product_performance`.
- Gold calculations consider **only** Silver rows with `order_status = 'Completed'`. The Gold scripts calculate gross sales as `quantity * unit_price`, net sales as `order_amount`, and discounts as gross minus net.
- Customer 360 and Product Performance preserve all valid customer/product master rows, including those with no completed orders.
- Bootstrap quarantine is append-only Parquet at a configured S3 path; it is **not** the incremental Iceberg quarantine table.
- S3 Parquet audit paths are written by the jobs, but the scripts do **not** create Athena audit tables. Do not assume `audit_*` SQL tables exist.

## How to use these files

1. Verify the actual database and table names.
2. Run in **Amazon Athena engine version 3** (or a compatible engine with Iceberg support) and select an appropriate Athena result location.
3. Capture query execution ID, execution timestamp, query text, row counts, and results. 
4. Treat a zero-row exception query as a **pass for that check**, not proof of all pipeline correctness.
5. Run reconciliations after a **known completed Gold refresh**, because Silver may change between Gold and Athena reads.

## Query index

| File | Validation | Expected result / interpretation |
|---|---|---|
| [`01_silver_integrity.sql`](01_silver_integrity.sql) | Silver order IDs, null keys, versions, statuses | `duplicate_order_ids = 0`, `null_order_ids = 0`; investigate other nonzero counts |
| [`02_quarantine_summary.sql`](02_quarantine_summary.sql) | Incremental quarantine categories, reason IDs, replay keys | Examine categories; `duplicate_quarantine_keys = 0` expected from MERGE key logic |
| [`03_daily_sales_reconciliation.sql`](03_daily_sales_reconciliation.sql) | Silver Completed vs Daily Sales Gold | Order and net-sales differences should be zero for a consistent snapshot |
| [`04_customer_360_reconciliation.sql`](04_customer_360_reconciliation.sql) | Silver Completed vs Customer 360 Gold | Order and net-sales differences should be zero; customer IDs unique |
| [`05_product_performance_reconciliation.sql`](05_product_performance_reconciliation.sql) | Silver Completed vs Product Performance Gold | Order and net-sales differences should be zero; product IDs unique |
| [`06_gold_cross_checks.sql`](06_gold_cross_checks.sql) | Three Gold net-sales totals | All three should match for a consistent refresh snapshot |

**Important grain assumption:** The Gold scripts sum `countDistinct(order_id)` by day/customer/product and then sum the groups. This equals a global distinct-order count **only when each order belongs to a single day, customer, and product grouping respectively**. The scripts enforce reconciliation and fail when this assumption is violated; the SQL reproduces those implemented checks rather than claiming the model handles multi-product order lines.

## Implementation-to-validation traceability

- `orders_bootstrap.py`: historical normalization, 12 DQ rules, duplicate-key resolution, Silver append, input/Silver count reconciliation.
- `orders_etl_v2_hardened_v4.py`: 13 DQ rules, batch winner selection, Silver `MERGE`, Iceberg quarantine `MERGE`, no-new-data audit, and optional post-Silver-merge failure injection.
- `gold_daily_sales_v1.py`: daily Completed-only aggregate, `INSERT OVERWRITE`, order/net-sales reconciliation.
- `gold_customer_360_v2.py`: left join customer master, Completed-only lifetime metrics, `INSERT OVERWRITE`, count/uniqueness/order/net-sales reconciliation.
- `gold_product_performance_v1.py`: left join product master, Completed-only performance, estimated COGS/margin, `INSERT OVERWRITE`, count/uniqueness/order/net-sales reconciliation.

