# ADR-008: Use Glue workflow for success orchestration and EventBridge/SNS for failure alerts

- **Status:** Accepted — reconstructed from frozen V1 implementation (not a new design approval)
- **Scope:** AWS Retail Data Engineering V1
- **Evidence basis:** Workflow and log-group screenshot evidence; failure-alert configuration is recorded architecture pending console verification.

## Context and decision

Workflow `retail-data-platform-v1` gates crawler → incremental job → three parallel Gold jobs through conditional success triggers; logging and failure notification are separate concerns.

## Rationale reflected by implementation

CloudWatch Logs `/aws-glue/jobs` is visible in job screenshots. The frozen failure-alert design routes Glue FAILED/TIMEOUT/STOPPED state changes through EventBridge to SNS email.

## Consequences and limitations

Do not depict CloudWatch logs as a serial dependency in the workflow, or imply that SNS alerts prove downstream recovery. Exact EventBridge rule, SNS subscription, and some trigger predicates have not been screenshot-verified.

## Verification boundary

This ADR records the existing V1 choice; it is **not** a claim that alternatives were formally evaluated in the original project or that unprovided AWS runtime evidence exists. Confirm deployed configuration and results independently before promoting them to verified evidence.

## Related project sources

- [`orders_bootstrap.py`](../../scripts/bootstrap/orders_bootstrap.py)
- [`orders_etl_v2_hardened_v4.py`](../../scripts/incremental/orders_etl_v2_hardened_v4.py)
- [Data quality and quarantine](../data-quality/README.md)
- [Orchestration](../../orchestration/README.md)
- [Monitoring](../../monitoring/README.md)
