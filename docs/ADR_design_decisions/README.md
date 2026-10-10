
# Architecture Decision Records — AWS Data Engineering 

## Decision register

| ADR | Decision | Status |
|---|---|---|
| [ADR-001](ADR-001-separate-historical-bootstrap-from-recurring-incremental-workflow.md) | Separate historical bootstrap from recurring incremental workflow |
| [ADR-002](ADR-002-use-aws-glue-data-catalog-and-crawler-for-incremental-source-discovery.md) | Use AWS Glue Data Catalog and crawler for incremental source discovery | 
| [ADR-003](ADR-003-use-bookmark-aware-glue-incremental-ingestion.md) | Use bookmark-aware Glue incremental ingestion |
| [ADR-004](ADR-004-store-silver-and-gold-datasets-as-apache-iceberg-tables.md) | Store Silver and Gold datasets as Apache Iceberg tables | 
| [ADR-005](ADR-005-enforce-explicit-dq-checks-and-separate-rejection-paths.md) | Enforce explicit DQ checks and separate rejection paths | 
| [ADR-006](ADR-006-resolve-order-versions-by-timestamp-and-business-hash.md) | Resolve order versions by timestamp and business hash |
| [ADR-007](ADR-007-refresh-three-gold-marts-from-current-silver-state.md) | Refresh three Gold marts from current Silver state | 
| [ADR-008](ADR-008-use-glue-workflow-for-success-orchestration-and-eventbridge-sns-for-failure-alerts.md) | Use Glue workflow for success orchestration and EventBridge/SNS for failure alerts | 
| [ADR-009](ADR-009-use-audits-and-reconciliations-without-claiming-cross-system-atomicity.md) | Use audits and reconciliations without claiming cross-system atomicity | 

## How to use these ADRs

Read alongside the [orchestration](../../orchestration/README.md), [data-quality](../data-quality/README.md), and [monitoring](../../monitoring/README.md) documents. 

