-- =============================================================================
-- Landing: raw CSV files -> transient all-STRING table (schema-on-read).
--
-- `raw_ext` is a TEMPORARY external table defined on the query job (source URIs
-- and the STRING schema from schemas/bigquery/landing/raw_telemetry.json), so
-- nothing permanent points at the raw files. The query result is written to a
-- run-scoped landing table (WRITE_TRUNCATE), which makes re-runs safe.
--
-- Lineage columns are added per row:
--   source_file : the object the row came from (_FILE_NAME pseudo-column)
--   ingested_at : that object's GCS creation time, passed as parallel arrays
-- Params: @uris ARRAY<STRING>, @created ARRAY<TIMESTAMP>  (same order/length)
-- =============================================================================
WITH files AS (
  SELECT uri, created
  FROM UNNEST(@uris) AS uri WITH OFFSET AS i
  JOIN UNNEST(@created) AS created WITH OFFSET AS j
    ON i = j
)
SELECT
  r.*,
  r._FILE_NAME AS source_file,
  f.created AS ingested_at
FROM raw_ext AS r
JOIN files AS f
  ON f.uri = r._FILE_NAME
