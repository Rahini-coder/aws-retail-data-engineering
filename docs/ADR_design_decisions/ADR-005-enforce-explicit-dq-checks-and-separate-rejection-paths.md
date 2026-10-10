# ADR-005: Enforce explicit DQ checks and separate rejection paths

- **Status:** Accepted — reconstructed from frozen V1 implementation (not a new design approval)
- **Scope:** AWS Retail Data Engineering V1
- **Evidence basis:** Source-code confirmed; rejection counts and sample Athena evidence pending.

## Context and decision

The incremental script applies 13 DQ checks, records failed rule IDs/reasons, then checks version conflicts. Bootstrap applies 12 checks because its version timestamp is assigned by the job.

## Rationale reflected by implementation

Incremental categories are `DATA_QUALITY`, `VERSION_CONFLICT`, and `TARGET_VERSION_CONFLICT` in an Iceberg quarantine table; bootstrap uses Parquet categories `BOOTSTRAP_DATA_QUALITY`, `BOOTSTRAP_KEY_CONFLICT`, and `BOOTSTRAP_DUPLICATE_COPY`.

## Consequences and limitations

Incremental superseded rows and stale/duplicate Silver candidates are counted but not quarantined. Rules do not validate amount arithmetic; null checks are not general whitespace checks. Quarantine storage formats are intentionally different in this V1.

## Verification boundary

This ADR records the existing V1 choice; it is **not** a claim that alternatives were formally evaluated in the original project or that unprovided AWS runtime evidence exists. Confirm deployed configuration and results independently before promoting them to verified evidence.

## Related project sources

- [`orders_bootstrap.py`](../../scripts/bootstrap/orders_bootstrap.py)
- [`orders_etl_v2_hardened_v4.py`](../../scripts/incremental/orders_etl_v2_hardened_v4.py)
- [Data quality and quarantine](../data-quality/README.md)
- [Orchestration](../../orchestration/README.md)
- [Monitoring](../../monitoring/README.md)
