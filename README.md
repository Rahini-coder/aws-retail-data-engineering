# AWS Retail Data Engineering Platform

### End-to-End Data Lakehouse | AWS Glue | PySpark | Apache Iceberg | Amazon Athena

An AWS-based retail data engineering project implementing historical data ingestion, incremental order processing, data quality validation, Apache Iceberg-based data storage, analytical data models, and automated workflow orchestration.

The project demonstrates how raw retail data can be transformed into curated, analytics-ready datasets using AWS-managed data engineering services.

**Key engineering capabilities:** Incremental ETL · Data Quality · Iceberg MERGE · Change Detection · Quarantine · Workflow Orchestration · Observability · SQL Analytics

---
## Solution Architecture

The platform follows a layered AWS data lakehouse
architecture, transforming raw retail data into
validated Silver and curated Gold datasets.

![AWS Retail Data Engineering Architecture](architecture-diagrams/AWS%20End-to-End%20Data%20Architecture.png)

[View all architecture diagrams](architecture-diagrams/)


### Architecture layers

| Layer | Implementation | Purpose |
|---|---|---|
| Source / Raw | Amazon S3 | Stores historical reference data, historical orders, and incremental order batches |
| Metadata | AWS Glue Data Catalog and Crawler | Maintains source metadata for downstream processing |
| Processing | AWS Glue and PySpark | Performs validation, transformation, incremental processing, and aggregation |
| Silver | Apache Iceberg | Maintains validated order records with incremental MERGE operations |
| Gold | Apache Iceberg | Provides curated analytical tables |
| Analytics | Amazon Athena | Executes SQL queries for validation, reconciliation, and business analysis |
| Orchestration | AWS Glue Workflow | Coordinates crawler, Silver processing, and parallel Gold jobs |
| Monitoring | Amazon CloudWatch, EventBridge, SNS | Captures job logs and routes job failure notifications |

---

## 2. Business Problem

Retail businesses generate transactional and reference data across customers, products, and orders.

To support reliable business analysis, this data must be validated, processed incrementally, reconciled, and organized into analytical models.

This project addresses several common data engineering challenges:

- Processing new and changed orders without repeatedly loading the entire historical dataset.
- Detecting duplicates, stale records, and conflicting updates.
- Preventing invalid records from entering trusted analytical datasets.
- Maintaining consistent analytical outputs across multiple business domains.
- Coordinating dependent processing jobs and capturing operational failures.

---

## 3. End-to-End Data Processing

### Historical bootstrap

A separate bootstrap process initializes the Silver Iceberg orders table from historical source data.

The historical bootstrap is distinct from the recurring incremental workflow.

### Incremental processing workflow

```text
On-Demand Workflow Start
           |
           v
Incremental Orders Crawler
           |
           v
Conditional Trigger: Crawler Success
           |
           v
Incremental Silver Glue Job
           |
           v
Conditional Trigger: Silver Success
           |
     +-----+-------------------+
     |             |           |
     v             v           v
Daily Sales    Customer 360   Product Performance
Gold Job       Gold Job       Gold Job
```

The three Gold jobs run in parallel after successful completion of incremental Silver processing.

The incremental Glue job uses AWS Glue job bookmarks to track processed input files.

---

## 4. Incremental Data Engineering Design

The incremental pipeline implements record-level validation, change detection, and version-aware processing.

### Change classification

| Classification | Meaning |
|---|---|
| INSERT | New order not present in the Silver target |
| UPDATE | Existing order with an eligible newer version |
| DUPLICATE | Repeated record or version |
| STALE | Incoming record older than the applicable version |
| CONFLICT | Incompatible records or competing versions |

The implementation uses timestamps and record hashes to evaluate incoming records.

Validated changes are applied to the Silver Iceberg table using MERGE operations. Physical updates are restricted by the implemented hash comparison logic.

### Data quality and quarantine

The incremental pipeline implements **13 data quality rules**, covering areas including:

- Required identifiers and fields.
- Order dates and timestamps.
- Quantity, price, discount, and amount validation.
- Permitted order statuses.
- Customer and product reference validation.

Rejected records are separated from trusted Silver data.

The implementation distinguishes between data quality failures and version-related conflicts, including `DATA_QUALITY`, `VERSION_CONFLICT`, and `TARGET_VERSION_CONFLICT`.

Historical bootstrap quarantine is stored as Parquet on Amazon S3. Incremental quarantine uses an Apache Iceberg table.

The pipeline also produces audit information for processing and reconciliation.

---

## 5. Curated Gold Data Models

The project implements three analytical Gold tables.

| Gold table | Analytical purpose |
|---|---|
| `gold_daily_sales` | Daily sales performance and transaction metrics |
| `gold_customer_360` | Customer-level purchasing behavior and activity |
| `gold_product_performance` | Product-level sales and performance analysis |

Gold processing uses a full-refresh `INSERT OVERWRITE` approach.

Business sales aggregations are restricted to orders with `Completed` status.

The resulting tables are queried using Amazon Athena.

---

## 6. Orchestration and Operational Monitoring

The project uses an AWS Glue Workflow to coordinate dependent processing steps.

**Workflow:** `retail-data-platform-v1`

- On-demand workflow initiation.
- Incremental crawler execution.
- Conditional progression after successful crawler completion.
- Incremental Silver processing.
- Conditional launch of three parallel Gold jobs.

### Failure handling

AWS Glue job execution logs are available through Amazon CloudWatch Logs.

Glue job failure-related state events are handled through Amazon EventBridge and routed to Amazon SNS for email notifications.

The monitoring design covers job states including `FAILED`, `TIMEOUT`, and `STOPPED`.

This design separates workflow dependency management from operational failure notification.

---

## 7. Data Validation and SQL Analytics

Amazon Athena is used to query the curated datasets.

The repository includes SQL for:

- Data reconciliation and validation.
- Daily sales analysis.
- Customer behavior analysis.
- Product performance analysis.
- Cross-domain analytical questions.

See [SQL Queries](sql/).

---

## 8. Technology Stack

| Technology | Role |
|---|---|
| Amazon S3 | Raw data and supporting data storage |
| AWS Glue 5.1 | PySpark-based ETL processing |
| AWS Glue Data Catalog | Metadata management |
| AWS Glue Crawlers | Incremental source discovery |
| AWS Glue Workflows | Pipeline orchestration |
| Apache Iceberg | Silver and Gold table format |
| Amazon Athena | Serverless SQL analytics |
| Amazon CloudWatch Logs | Job execution logging |
| Amazon EventBridge | Job state event routing |
| Amazon SNS | Email failure notifications |
| Python / PySpark / SQL | Data transformation and analytical querying |

---

## 9. Repository Structure

| Directory | Contents |
|---|---|
| [Architecture Diagrams](architecture-diagrams/) | End-to-end architecture and focused technical diagrams |
| [Scripts](scripts/) | Historical bootstrap, incremental ETL, and Gold processing scripts |
| [SQL](sql/) | Reconciliation and analytical queries |
| [Sample Data](sample-data/) | Synthetic historical and incremental datasets with expected scenarios |
| [Documentation](docs/) | Data quality documentation and architecture decision records |
| [Orchestration](orchestration/) | Workflow design documentation |
| [Monitoring](monitoring/) | Logging and failure-notification design |

---

## 10. Architecture Decision Records

The project documents significant engineering decisions through Architecture Decision Records (ADRs).

These records explain the rationale behind the selected processing patterns, data storage approaches, quality controls, and operational design.

See [Design Decisions](docs/design-decisions/).

---

## 11. Sample Data and Reproducibility

The repository provides synthetic retail data to illustrate the processing scenarios.

The sample-data package includes historical customers, products, and orders, along with incremental order batches.

Documented test scenarios cover new records, changed records, duplicates, stale versions, validation failures, and version conflicts.

See [Sample Data](sample-data/).

---

## 12. Implementation Scope and Limitations

This repository documents the V1 implementation of the AWS retail data engineering platform.

The implemented analytics consumption layer is Amazon Athena. A BI dashboard and machine learning deployment are not part of V1.

The design uses Glue job bookmarks and Iceberg operations for incremental processing. It does not claim a single atomic transaction across Silver, quarantine, audit, and bookmark state.

---

## 13. Project Outcomes

The project demonstrates practical implementation of:

- AWS data lakehouse architecture.
- Historical and incremental data processing.
- PySpark transformation and data quality validation.
- Apache Iceberg MERGE and curated analytical modeling.
- Data versioning and conflict-handling strategies.
- AWS Glue workflow orchestration.
- Event-driven operational failure notifications.
- SQL-based analytics and reconciliation.
- Engineering design documentation through ADRs.

---

## Project Status

**Implementation documented.**

Architecture diagrams, ETL scripts, SQL queries, design decisions, and synthetic sample data are organized within the repository.

---

*This project uses synthetic retail data for portfolio and learning purposes.*
