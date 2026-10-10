# Synthetic Sample Data — AWS Retail Data Engineering 

## Files and intended loading order

1. `historical/customers.csv` and `historical/products.csv`: illustrative master reference data; register as Glue Catalog sources before jobs run.
2. `historical/orders.csv`: historical bootstrap order input; use the separate bootstrap Glue job **once against an empty Silver target**. Its `source_updated_at` is set by `--bootstrap_version_timestamp` (not read from CSV). For the examples below, assume `2026-09-02 00:00:00`.
3. `incremental/orders_batch_001.csv` through `_003.csv`: upload **one batch at a time** under the configured `raw/orders_incremental/` prefix and execute the crawler/workflow after each batch. Do not place all batches in the prefix before the first run if you want to reproduce the staged outcomes.

The original V1 raw prefixes are `raw/customers/`, `raw/products/`, `raw/orders/orders.csv`, and `raw/orders_incremental/orders_batch_XXX.csv` in `s3://rahini-aws-data-engineering-2026/`. 

**Nuances:** Intra-batch superseded records are audited but not quarantined. Stale records and duplicates against Silver are classified/audited, not quarantined. Quarantine is an incremental Iceberg table keyed for replay-aware MERGE; bootstrap quarantine, if present, is written as partitioned Parquet. The Glue bookmark, Iceberg writes and audit output do not share a global transaction.

## Schema notes

Order CSV header: `order_id,customer_id,order_date,product_id,quantity,unit_price,discount_pct,order_amount,order_status,payment_method`; incremental batches additionally require `source_updated_at`. `discount_pct` uses 0–100 units. Order statuses accepted by the incremental script are `Completed`, `Cancelled`, `Returned`, `Pending`.

See `expected-results/test-scenarios.md` for record-level expectations. Amounts are examples; 
