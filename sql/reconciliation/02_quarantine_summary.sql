-- Incremental Iceberg quarantine only (NOT bootstrap Parquet quarantine).

SELECT quarantine_category, COUNT(*) AS records,
       COUNT(DISTINCT quarantine_key) AS distinct_quarantine_keys
FROM "customer_intelligence"."orders_quarantine"
GROUP BY quarantine_category
ORDER BY quarantine_category;

-- Expected zero duplicate quarantine keys (assuming successful MERGE semantics).
SELECT quarantine_key, COUNT(*) AS copies
FROM "customer_intelligence"."orders_quarantine"
GROUP BY quarantine_key
HAVING COUNT(*) > 1;

-- Explode recorded DQ IDs (categories other than DATA_QUALITY may have empty arrays).
SELECT rule_id, COUNT(*) AS affected_records
FROM "customer_intelligence"."orders_quarantine"
CROSS JOIN UNNEST(dq_rule_ids) AS t(rule_id)
GROUP BY rule_id
ORDER BY rule_id;
