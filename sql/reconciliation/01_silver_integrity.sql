-- V1 Silver integrity: replace <silver_database> with the deployed database.
-- The script-defined table name is normally silver_orders.
WITH s AS (
  SELECT * FROM "<silver_database>"."silver_orders"
), duplicate_keys AS (
  SELECT order_id FROM s GROUP BY order_id HAVING COUNT(*) > 1
)
SELECT
  (SELECT COUNT(*) FROM s) AS silver_rows,
  (SELECT COUNT(*) FROM s WHERE order_id IS NULL) AS null_order_ids,
  (SELECT COUNT(*) FROM duplicate_keys) AS duplicate_order_ids,
  (SELECT COUNT(*) FROM s WHERE source_updated_at IS NULL) AS missing_source_versions,
  (SELECT COUNT(*) FROM s WHERE record_hash IS NULL) AS missing_record_hashes,
  (SELECT COUNT(*) FROM s WHERE order_status NOT IN ('Completed','Cancelled','Returned','Pending') OR order_status IS NULL) AS invalid_statuses;
