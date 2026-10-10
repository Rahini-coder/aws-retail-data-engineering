# ADR-009: Use audits and reconciliations without claiming cross-system atomicity

## Context and decision

Bootstrap and incremental jobs append operational audit records to S3 Parquet; Gold jobs record reconciliation audit results. Athena validation SQL is a separate verification layer.

## Rationale reflected by implementation

The incremental job records DQ rejected, version conflicts, superseded, insert/update/duplicate/stale, target conflicts and no-new-data outcomes. A controlled `AFTER_SILVER_MERGE` failure injection exists in source to examine replay.

## Consequences and limitations

An injected failure may occur after a committed Silver MERGE but before bookmark commit; quarantine, Silver, audit and bookmark are not a single transaction. Audit counts and physical Silver mutations can differ in the unchanged-hash newer-version case.


## Related project sources

- [`orders_bootstrap.py`](../../scripts/bootstrap/orders_bootstrap.py)
- [`orders_etl_v2_hardened_v4.py`](../../scripts/incremental/orders_etl_v2_hardened_v4.py)
- [Data quality and quarantine](../data-quality/README.md)
- [Orchestration](../../orchestration/README.md)
- [Monitoring](../../monitoring/README.md)
