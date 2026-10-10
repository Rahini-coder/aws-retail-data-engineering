# ADR-009: Use audits and reconciliations without claiming cross-system atomicity

- **Status:** Accepted — reconstructed from frozen V1 implementation (not a new design approval)
- **Scope:** AWS Retail Data Engineering V1
- **Evidence basis:** Source-code confirmed; specific injected-failure test outcomes and Athena results must not be invented.

## Context and decision

Bootstrap and incremental jobs append operational audit records to S3 Parquet; Gold jobs record reconciliation audit results. Athena validation SQL is a separate verification layer.

## Rationale reflected by implementation

The incremental job records DQ rejected, version conflicts, superseded, insert/update/duplicate/stale, target conflicts and no-new-data outcomes. A controlled `AFTER_SILVER_MERGE` failure injection exists in source to examine replay.

## Consequences and limitations

An injected failure may occur after a committed Silver MERGE but before bookmark commit; quarantine, Silver, audit and bookmark are not a single transaction. Audit counts and physical Silver mutations can differ in the unchanged-hash newer-version case.

## Verification boundary

This ADR records the existing V1 choice; it is **not** a claim that alternatives were formally evaluated in the original project or that unprovided AWS runtime evidence exists. Confirm deployed configuration and results independently before promoting them to verified evidence.

## Related project sources

- [`orders_bootstrap.py`](../../scripts/bootstrap/orders_bootstrap.py)
- [`orders_etl_v2_hardened_v4.py`](../../scripts/incremental/orders_etl_v2_hardened_v4.py)
- [Data quality and quarantine](../data-quality/README.md)
- [Orchestration](../../orchestration/README.md)
- [Monitoring](../../monitoring/README.md)
