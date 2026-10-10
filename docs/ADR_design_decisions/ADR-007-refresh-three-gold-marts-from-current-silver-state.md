# ADR-007: Refresh three Gold marts from current Silver state


## Context and decision

Three Glue Gold jobs read Silver and create `gold_daily_sales`, `gold_customer_360`, and `gold_product_performance` using full-refresh `INSERT OVERWRITE` patterns.

## Rationale reflected by implementation

Only `Completed` orders contribute to sales metrics. Customer and product marts retain reference-master entities without completed purchases. Gold jobs perform Silver-versus-Gold count and net-sales reconciliation and write Parquet audit records.

## Consequences and limitations

Full refresh trades simplicity and consistent current-state calculations for repeated scans. Product estimated cost/margin reflects current product master cost, not necessarily historical transaction-time cost. Exact deployed Customer 360 script path is pending verification.

## Related project sources

- [`orders_bootstrap.py`](../../scripts/bootstrap/orders_bootstrap.py)
- [`orders_etl_v2_hardened_v4.py`](../../scripts/incremental/orders_etl_v2_hardened_v4.py)
- [Data quality and quarantine](../data-quality/README.md)
- [Orchestration](../../orchestration/README.md)
- [Monitoring](../../monitoring/README.md)
