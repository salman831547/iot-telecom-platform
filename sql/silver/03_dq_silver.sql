-- =============================================================================
-- Silver data-quality checks for the batches written by this run.
-- Thresholds are evaluated in Python against config; this query only measures.
-- Params: @dates ARRAY<DATE>, @batch_ids ARRAY<STRING>
-- =============================================================================
WITH run_rows AS (
  SELECT *
  FROM `${silver_table}`
  WHERE event_date IN UNNEST(@dates)
    AND batch_id IN UNNEST(@batch_ids)
),
per_batch AS (
  SELECT batch_id, COUNT(*) AS n FROM run_rows GROUP BY batch_id
),
partition_keys AS (
  -- Uniqueness must hold across the whole affected partitions, not just this run.
  SELECT COUNT(*) - COUNT(DISTINCT event_id) AS duplicate_event_ids
  FROM `${silver_table}`
  WHERE event_date IN UNNEST(@dates)
)
SELECT
  (SELECT COUNT(*) FROM run_rows)                                  AS rows_total,
  (SELECT COUNTIF(NOT is_valid) FROM run_rows)                     AS rows_invalid,
  (SELECT COUNTIF(event_id IS NULL OR device_id IS NULL OR event_ts IS NULL) FROM run_rows)
                                                                   AS null_keys,
  (SELECT duplicate_event_ids FROM partition_keys)                 AS duplicate_event_ids,
  (SELECT IFNULL(MIN(n), 0) FROM per_batch)                        AS min_rows_per_batch,
  (SELECT COUNT(*) FROM per_batch)                                 AS batches_found
