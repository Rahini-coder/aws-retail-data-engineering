# ADR-002: Use AWS Glue Data Catalog and crawler for incremental source discovery

- **Status:** Accepted — reconstructed from frozen V1 implementation (not a new design approval)
- **Scope:** AWS Retail Data Engineering V1
- **Evidence basis:** Owner-confirmed settings and screenshot of crawler update; exact trigger predicate pending.

## Context and decision

The recurring workflow starts a crawler over the incremental orders S3 prefix, then conditionally starts the incremental ETL after crawler success.

## Rationale reflected by implementation

Crawler `retail-orders-incremental` targets `raw/orders_incremental/`, updates Data Catalog database `customer_intelligence` and table `incremental_orders_incremental`; owner confirmed `Crawl all folders`, update table definition, and mark deleted tables deprecated.

## Consequences and limitations

Crawler refreshes metadata, not the source files; recrawling all folders can add crawl work. The exact conditional trigger predicates remain to be verified from configuration screenshots.

## Verification boundary

This ADR records the existing V1 choice; it is **not** a claim that alternatives were formally evaluated in the original project or that unprovided AWS runtime evidence exists. Confirm deployed configuration and results independently before promoting them to verified evidence.

## Related project sources

- [`orders_bootstrap.py`](../../scripts/bootstrap/orders_bootstrap.py)
- [`orders_etl_v2_hardened_v4.py`](../../scripts/incremental/orders_etl_v2_hardened_v4.py)
- [Data quality and quarantine](../data-quality/README.md)
- [Orchestration](../../orchestration/README.md)
- [Monitoring](../../monitoring/README.md)
