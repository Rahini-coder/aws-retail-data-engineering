# ADR-001: Separate historical bootstrap from recurring incremental workflow


## Context and decision

The historical order population is loaded by a separate one-time Glue PySpark bootstrap job; it is not a node in `retail-data-platform-v1`.

## Rationale reflected by implementation

The bootstrap initializes an empty Iceberg `silver_orders`, assigns a supplied `bootstrap_version_timestamp`, applies 12 DQ checks, resolves bootstrap duplicate/key conflicts, and refuses to append if Silver is nonempty. The recurring workflow processes new incremental files separately.

## Consequences and limitations

A one-time empty-target guard protects against accidentally replaying the bootstrap, but bootstrap is not a general-purpose upsert or backfill mechanism. Bootstrap rejected rows are appended as partitioned Parquet, unlike incremental quarantine.


## Related project sources

- [`orders_bootstrap.py`](../../scripts/bootstrap/orders_bootstrap.py)
- [`orders_etl_v2_hardened_v4.py`](../../scripts/incremental/orders_etl_v2_hardened_v4.py)
- [Data quality and quarantine](../data-quality/README.md)
- [Orchestration](../../orchestration/README.md)
- [Monitoring](../../monitoring/README.md)
