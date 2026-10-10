# ADR-004: Store Silver and Gold datasets as Apache Iceberg tables


## Context and decision

The bootstrap creates Iceberg `silver_orders`; incremental loads use SQL MERGE for Silver; Gold jobs create Iceberg v2 tables and refresh with `INSERT OVERWRITE`.

## Rationale reflected by implementation

Iceberg supports key-based Silver change application and table-level refresh semantics used in the uploaded scripts. Silver and incremental quarantine are partitioned by `months(order_date)`.

## Consequences and limitations

Individual Iceberg operations do not make the complete Glue job atomic across Silver, quarantine, audit and bookmarks. Avoid claiming cross-table ACID guarantees.

## Related project sources

- [`orders_bootstrap.py`](../../scripts/bootstrap/orders_bootstrap.py)
- [`orders_etl_v2_hardened_v4.py`](../../scripts/incremental/orders_etl_v2_hardened_v4.py)
- [Data quality and quarantine](../data-quality/README.md)
- [Orchestration](../../orchestration/README.md)
- [Monitoring](../../monitoring/README.md)
