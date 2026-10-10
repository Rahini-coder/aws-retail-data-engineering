# AWS Data Engineering Project — Amazon S3 Bucket Structure

**S3 bucket:** `s3://rahini-aws-data-engineering-2026/`

This document records the Amazon S3 layout for the data engineering project. The tree shows S3 object keys and logical prefixes; folders in S3 are prefixes rather than traditional directories.

```text
s3://rahini-aws-data-engineering-2026/
├── raw/
│   ├── customers/
│   ├── products/
│   ├── orders/
│   │   └── orders.csv
│   └── orders_incremental/
│       ├── orders_batch_001.csv
│       ├── orders_batch_002.csv
│       ├── orders_batch_003.csv
│       └── orders_batch_004.csv
├── scripts/
│   ├── bootstrap/
│   │   └── orders_bootstrap.py
│   ├── incremental/
│   │   └── orders_etl_v2_hardened_v4.py
│   └── gold/
│       ├── gold_daily_sales_v1.py
│       ├── gold_customer_360_v2.py
│       └── gold_product_performance_v1.py
├── warehouse/
│   └── customer_intelligence/
│       ├── silver_orders/
│       ├── orders_quarantine/
│       ├── gold_daily_sales/
│       ├── gold_customer_360/
│       └── gold_product_performance/
├── audit/
│   ├── orders/
│   ├── orders_incremental/
│   ├── gold_daily_sales/
│   ├── gold_customer_360/
│   └── gold_product_performance/
└── athena-results/
```

## Prefix overview

| Prefix | Purpose |
|---|---|
| `raw/` | Source datasets, including historical orders and incremental batches. |
| `scripts/` | Glue/PySpark scripts for bootstrap, incremental processing, and Gold jobs. |
| `warehouse/customer_intelligence/` | Silver, quarantine, and Gold data storage. |
| `audit/` | Audit outputs organized by pipeline or dataset. |
| `athena-results/` | Amazon Athena query results. |


