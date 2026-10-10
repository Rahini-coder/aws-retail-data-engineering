# ADR-007: Refresh three Gold marts from current Silver state

- **Status:** Accepted — reconstructed from frozen V1 implementation (not a new design approval)
- **Scope:** AWS Retail Data Engineering V1
- **Evidence basis:** Source-code confirmed; separate Gold job run screenshots exist, but full same-workflow run correlation remains incomplete.

## Context and decision

Three Glue Gold jobs read Silver and create `gold_daily_sales`, `gold_customer_360`, and `gold_product_performance` using full-refresh `INSERT OVERWRITE` patterns.

## Rationale reflected by implementation

Only `Completed` orders contribute to sales metrics. Customer and product marts retain reference-master entities without completed purchases. Gold jobs perform Silver-versus-Gold count and net-sales reconciliation and write Parquet audit records.

## Consequences and limitations

Full refresh trades simplicity and consistent current-state calculations for repeated scans. Product estimated cost/margin reflects current product master cost, not necessarily historical transaction-time cost. Exact deployed Customer 360 script path is pending verification.

## Verification boundary

This ADR records the existing V1 choice; it is **not** a claim that alternatives were formally evaluated in the original project or that unprovided AWS runtime evidence exists. Confirm deployed configuration and results independently before promoting them to verified evidence.

## Related project sources

- [`orders_bootstrap.py`](../../scripts/bootstrap/orders_bootstrap.py)
- [`orders_etl_v2_hardened_v4.py`](../../scripts/incremental/orders_etl_v2_hardened_v4.py)
- [Data quality and quarantine](../data-quality/README.md)
- [Orchestration](../../orchestration/README.md)
- [Monitoring](../../monitoring/README.md)
