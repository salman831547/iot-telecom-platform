-- =============================================================================
-- Gold: rebuild ONLY the partitions affected by the latest silver load, in one
-- transaction (delete + insert). Deterministic from silver, so re-runs are safe,
-- and late-arriving rows simply trigger a rebuild of their day.
-- KPIs use valid rows only; invalid rows are counted separately.
-- Params: @dates ARRAY<DATE>, @poor_rsrp_threshold FLOAT64
-- =============================================================================
BEGIN TRANSACTION;

DELETE FROM `${gold_device_table}` WHERE event_date IN UNNEST(@dates);

INSERT INTO `${gold_device_table}` (
  event_date, event_hour, device_id, device_type, network_type,
  event_count, invalid_event_count, alarm_count, distinct_cells,
  avg_rsrp_dbm, p10_rsrp_dbm, avg_sinr_db, min_battery_pct, battery_drain_pct,
  avg_temperature_c, max_temperature_c, total_bytes_up, total_bytes_down, updated_at
)
SELECT
  event_date,
  TIMESTAMP_TRUNC(event_ts, HOUR)                                         AS event_hour,
  device_id,
  ANY_VALUE(device_type)                                                  AS device_type,
  APPROX_TOP_COUNT(IF(is_valid, network_type, NULL), 1)[SAFE_OFFSET(0)].value AS network_type,
  COUNTIF(is_valid)                                                       AS event_count,
  COUNTIF(NOT is_valid)                                                   AS invalid_event_count,
  COUNTIF(event_type = 'ALARM')                                           AS alarm_count,
  COUNT(DISTINCT cell_id)                                                 AS distinct_cells,
  ROUND(AVG(IF(is_valid, rsrp_dbm, NULL)), 2)                             AS avg_rsrp_dbm,
  APPROX_QUANTILES(IF(is_valid, rsrp_dbm, NULL), 10)[SAFE_OFFSET(1)]      AS p10_rsrp_dbm,
  ROUND(AVG(IF(is_valid, sinr_db, NULL)), 2)                              AS avg_sinr_db,
  MIN(IF(is_valid, battery_pct, NULL))                                    AS min_battery_pct,
  ROUND(
    ARRAY_AGG(IF(is_valid, battery_pct, NULL) IGNORE NULLS ORDER BY event_ts ASC LIMIT 1)[SAFE_OFFSET(0)]
    - ARRAY_AGG(IF(is_valid, battery_pct, NULL) IGNORE NULLS ORDER BY event_ts DESC LIMIT 1)[SAFE_OFFSET(0)],
  2)                                                                      AS battery_drain_pct,
  ROUND(AVG(IF(is_valid, temperature_c, NULL)), 2)                        AS avg_temperature_c,
  MAX(IF(is_valid, temperature_c, NULL))                                  AS max_temperature_c,
  SUM(IF(is_valid, bytes_up, 0))                                          AS total_bytes_up,
  SUM(IF(is_valid, bytes_down, 0))                                        AS total_bytes_down,
  CURRENT_TIMESTAMP()                                                     AS updated_at
FROM `${silver_table}`
WHERE event_date IN UNNEST(@dates)
GROUP BY event_date, event_hour, device_id;

DELETE FROM `${gold_cell_table}` WHERE event_date IN UNNEST(@dates);

INSERT INTO `${gold_cell_table}` (
  event_date, event_hour, cell_id, mcc, mnc, tac, active_devices, event_count,
  avg_rsrp_dbm, p10_rsrp_dbm, avg_rsrq_db, avg_sinr_db, poor_coverage_ratio, total_bytes, updated_at
)
SELECT
  event_date,
  TIMESTAMP_TRUNC(event_ts, HOUR)                                  AS event_hour,
  cell_id,
  ANY_VALUE(mcc)                                                   AS mcc,
  ANY_VALUE(mnc)                                                   AS mnc,
  ANY_VALUE(tac)                                                   AS tac,
  COUNT(DISTINCT device_id)                                        AS active_devices,
  COUNT(*)                                                         AS event_count,
  ROUND(AVG(rsrp_dbm), 2)                                          AS avg_rsrp_dbm,
  APPROX_QUANTILES(rsrp_dbm, 10)[SAFE_OFFSET(1)]                   AS p10_rsrp_dbm,
  ROUND(AVG(rsrq_db), 2)                                           AS avg_rsrq_db,
  ROUND(AVG(sinr_db), 2)                                           AS avg_sinr_db,
  ROUND(SAFE_DIVIDE(COUNTIF(rsrp_dbm < @poor_rsrp_threshold), COUNT(rsrp_dbm)), 4) AS poor_coverage_ratio,
  SUM(IFNULL(bytes_up, 0) + IFNULL(bytes_down, 0))                 AS total_bytes,
  CURRENT_TIMESTAMP()                                              AS updated_at
FROM `${silver_table}`
WHERE event_date IN UNNEST(@dates)
  AND is_valid
  AND cell_id IS NOT NULL
GROUP BY event_date, event_hour, cell_id;

COMMIT TRANSACTION;

SELECT
  (SELECT COUNT(*) FROM `${gold_device_table}` WHERE event_date IN UNNEST(@dates)) AS device_rows,
  (SELECT COUNT(*) FROM `${gold_cell_table}` WHERE event_date IN UNNEST(@dates))   AS cell_rows;
