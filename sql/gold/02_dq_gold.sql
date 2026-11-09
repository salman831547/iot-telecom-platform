-- =============================================================================
-- Gold data-quality: reconcile gold back to silver for the rebuilt partitions.
-- Valid silver events must equal the sum of device-level event_count, and the
-- same for cells (valid rows with a known cell).
-- Params: @dates ARRAY<DATE>
-- =============================================================================
SELECT
  (SELECT COUNTIF(is_valid) FROM `${silver_table}` WHERE event_date IN UNNEST(@dates))
    AS silver_valid_events,
  (SELECT IFNULL(SUM(event_count), 0) FROM `${gold_device_table}` WHERE event_date IN UNNEST(@dates))
    AS gold_device_events,
  (SELECT COUNTIF(is_valid AND cell_id IS NOT NULL) FROM `${silver_table}` WHERE event_date IN UNNEST(@dates))
    AS silver_valid_cell_events,
  (SELECT IFNULL(SUM(event_count), 0) FROM `${gold_cell_table}` WHERE event_date IN UNNEST(@dates))
    AS gold_cell_events,
  (SELECT COUNTIF(event_count < 0 OR invalid_event_count < 0) FROM `${gold_device_table}`
     WHERE event_date IN UNNEST(@dates))
    AS negative_counts
