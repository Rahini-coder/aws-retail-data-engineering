# ADR-006: Resolve order versions by timestamp and business hash

- **Status:** Accepted — reconstructed from frozen V1 implementation (not a new design approval)
- **Scope:** AWS Retail Data Engineering V1
- **Evidence basis:** Source-code confirmed; scenario examples are logical deductions, not asserted live test results.

## Context and decision

The incremental job hashes nine business fields with SHA-256, rejects same-order/same-version different-hash batch conflicts, selects the latest remaining version per order, and classifies against Silver.

## Rationale reflected by implementation

Classification is INSERT, UPDATE, DUPLICATE, STALE or CONFLICT. Equal-timestamp/different-hash against Silver becomes `TARGET_VERSION_CONFLICT`. Silver MERGE updates only when incoming timestamp is newer **and** business hash differs.

## Consequences and limitations

A newer version with identical business hash is counted as UPDATE but not physically written to Silver; target version timestamp stays unchanged. This is a documented V1 nuance, not an enhancement proposal.

## Verification boundary

This ADR records the existing V1 choice; it is **not** a claim that alternatives were formally evaluated in the original project or that unprovided AWS runtime evidence exists. Confirm deployed configuration and results independently before promoting them to verified evidence.

## Related project sources

- [`orders_bootstrap.py`](../../scripts/bootstrap/orders_bootstrap.py)
- [`orders_etl_v2_hardened_v4.py`](../../scripts/incremental/orders_etl_v2_hardened_v4.py)
- [Data quality and quarantine](../data-quality/README.md)
- [Orchestration](../../orchestration/README.md)
- [Monitoring](../../monitoring/README.md)
