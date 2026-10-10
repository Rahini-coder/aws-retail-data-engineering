# ADR-003: Use bookmark-aware Glue incremental ingestion

- **Status:** Accepted — reconstructed from frozen V1 implementation (not a new design approval)
- **Scope:** AWS Retail Data Engineering V1
- **Evidence basis:** Script confirmed; bookmark enablement owner-reported; controlled replay run evidence pending.

## Context and decision

The incremental PySpark script reads the Data Catalog source using `glue.create_dynamic_frame.from_catalog` and a stable transformation context `orders_incremental_source`, and calls `job.commit()` on successful paths.

## Rationale reflected by implementation

The deployed Glue incremental job is owner-reported to have bookmarks enabled. The script explicitly handles no new data with a `NO_NEW_DATA` audit and commit.

## Consequences and limitations

Bookmarks manage input progress but do not create a transaction with Iceberg MERGE, quarantine, or audit. Replay protection also depends on business keys, timestamps and hashes; no exactly-once claim across outputs.

## Verification boundary

This ADR records the existing V1 choice; it is **not** a claim that alternatives were formally evaluated in the original project or that unprovided AWS runtime evidence exists. Confirm deployed configuration and results independently before promoting them to verified evidence.

## Related project sources

- [`orders_bootstrap.py`](../../scripts/bootstrap/orders_bootstrap.py)
- [`orders_etl_v2_hardened_v4.py`](../../scripts/incremental/orders_etl_v2_hardened_v4.py)
- [Data quality and quarantine](../data-quality/README.md)
- [Orchestration](../../orchestration/README.md)
- [Monitoring](../../monitoring/README.md)
