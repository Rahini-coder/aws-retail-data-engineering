# AWS Retail Data Engineering 

**A batch-oriented AWS lakehouse pipeline for retail order processing, incremental ingestion, data quality, analytics, and operational alerting.**

> **Project status:** Architecture frozen. This repository documents the established design; proposed improvements belong in a separately labeled future version. Code, screenshots, and validation evidence added only after verification against the implementation.

## Architecture at a glance

The solution is organized into **three interacting planes**:

| Plane | Responsibility | AWS components |
|---|---|---|
| **Data** | Ingest, validate, curate, and query retail data | Amazon S3, AWS Glue Crawler & Data Catalog, AWS Glue 5.1 / PySpark, Apache Iceberg, Amazon Athena |
| **Control**| Run the crawler, then incremental processing, then downstream Gold jobs after successful completion | AWS Glue triggers, crawler, and jobs |
| **Observability** | Collect execution logs and alert on selected failed job states | Amazon CloudWatch Logs, Amazon EventBridge, Amazon SNS, email |

### End-to-end data flow

```mermaid
flowchart LR
    S3[Amazon S3<br/>RAW] --> C[AWS Glue Crawler]
    C --> CAT[Glue Data Catalog]
    CAT -.-> B[Historical Bootstrap]
    CAT -.-> ETL[AWS Glue 5.1<br/>Incremental PySpark ETL]
    B --> SIL[Apache Iceberg<br/>silver_orders]
    ETL --> DQ{DQ & version checks}
    DQ -->|Accepted| SIL
    DQ -->|Rejected| Q[Iceberg Quarantine<br/>& audit evidence]
    SIL --> DS[Gold Daily Sales]
    SIL --> C360[Gold Customer 360]
    SIL --> PP[Gold Product Performance]
    DS --> ATH[Amazon Athena]
    C360 --> ATH
    PP --> ATH
```

*The Data Catalog supplies metadata; Glue reads source data from S3. Historical bootstrap is a separate one-time path from the bookmark-aware incremental job.*

### Control flow

```mermaid
flowchart TD
    START[retail-pipeline-start] --> CR[retail-orders-incremental<br/>Crawler]
    CR -->|SUCCEEDED| TC[retail-after-orders-crawler]
    TC --> ETL[retail-orders-incremental-v2<br/>Glue Job]
    ETL -->|SUCCEEDED| TG[retail-after-silver-success]
    TG --> G1[Daily Sales Job]
    TG --> G2[Customer 360 Job]
    TG --> G3[Product Performance Job]
```

### Observability flow

```mermaid
flowchart LR
    GLUE[Glue / PySpark execution] --> LOGS[CloudWatch Logs]
    GLUE --> STATE[Glue job state changes]
    STATE --> EB[EventBridge]
    EB -->|FAILED / TIMEOUT / STOPPED| SNS[Amazon SNS]
    SNS --> EMAIL[Email notification]
```

*The logging path and state-change notification path are separate. The alert path is for `FAILED`, `TIMEOUT`, and `STOPPED` events; orchestration success dependencies are separate.*

## Data layers

| Layer | Key datasets / outputs |
|---|---|
| RAW | Customers, products, historical orders, incremental order batches |
| Silver | Apache Iceberg `silver_orders` |
| Quarantine | Rejected orders and supporting audit evidence |
| Gold | Daily Sales, Customer 360, Product Performance |
| Query | Amazon Athena reconciliation and business queries |

Incremental processing includes data-quality and reference checks, version logic, and hash comparison. Accepted records feed Silver; rejected records are kept in quarantine. Audit outputs are organized by pipeline or dataset.

## Implementation inventory

The S3 layout includes the following named scripts:

```text
scripts/
├── bootstrap/
│   └── orders_bootstrap.py
├── incremental/
│   └── orders_etl_v2_hardened_v4.py
└── gold/
    ├── gold_daily_sales_v1.py
    ├── gold_customer_360_v2.py
    └── gold_product_performance_v1.py
```

## Repository guide

| Location | Contents |
|---|---|
| [`S3_BUCKET_STRUCTURE.md`](S3_BUCKET_STRUCTURE.md) | S3 prefix and object structure |
| `docs/architecture/` | architecture images |
| `scripts/` | Verified Glue/PySpark source code |
| `sql/` | Verified Athena reconciliation and analytics queries |
| `orchestration/` | Glue trigger configuration and execution evidence  |
| `monitoring/` | EventBridge/SNS configuration and log evidence |
| `docs/design-decisions/` | Architecture decision records |
| `athena/results` | verified results  |

## S3 storage layout

The documented bucket is `s3://rahini-aws-data-engineering-2026/`. Major prefixes are `raw/`, `scripts/`, `warehouse/customer_intelligence/`, `audit/`, and `athena-results/`. See the [full S3 bucket structure](S3_BUCKET_STRUCTURE.md).

## Engineering principles

- Preserve the distinction between **historical bootstrap** and **incremental processing**.
- Apply data-quality and version checks before accepting incremental records into Silver.
- Keep rejected records and audit evidence separate from accepted Silver data.
- Launch the three Gold jobs only after successful completion of the incremental Silver job.
- Separate operational logging from state-change alerting.
- Record evidence and design decisions.

---


