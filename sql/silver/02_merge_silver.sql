-- =============================================================================
-- Silver: clean + conform + de-duplicate landing rows, MERGE into silver, and
-- advance the incremental watermark, all in ONE transaction. Either the data
-- and the watermark both move, or neither does.
--
-- Safe to re-run: event_id is deterministic (device_id + event_ts), so
-- re-processing the same files updates rows in place instead of duplicating.
--
-- Identifiers (dollar-brace placeholders) come from validated config. Values are typed query params:
--   @pii_salt STRING, @watermark_key STRING, @new_watermark TIMESTAMP,
--   @last_batch_id STRING, @run_id STRING,
--   @rsrp_min/@rsrp_max, @rsrq_min/@rsrq_max, @sinr_min/@sinr_max,
--   @battery_min/@battery_max, @temp_min/@temp_max          FLOAT64
--   @lat_min/@lat_max/@lon_min/@lon_max                      FLOAT64
-- =============================================================================
DECLARE affected_dates ARRAY<DATE>;

-- ---- Reusable cleaning functions --------------------------------------------
-- Source "no value" tokens -> NULL; trims whitespace.
CREATE TEMP FUNCTION clean_str(x STRING) AS (
  IF(UPPER(TRIM(x)) IN ('', 'N/A', 'NULL', 'NAN', '-'), NULL, TRIM(x))
);
-- Strip unit suffixes ("-97.1 dBm", "77%", "31.5C") and cast.
CREATE TEMP FUNCTION clean_num(x STRING) AS (
  SAFE_CAST(REGEXP_REPLACE(clean_str(x), r'(?i)\s*(dbm|db|%|c)$', '') AS FLOAT64)
);
-- Three source formats: epoch milliseconds, ISO-8601 ('T' ... 'Z'), 'YYYY-MM-DD HH:MM:SS'.
CREATE TEMP FUNCTION parse_event_ts(x STRING) AS (
  IF(
    REGEXP_CONTAINS(clean_str(x), r'^\d{13}$'),
    TIMESTAMP_MILLIS(SAFE_CAST(clean_str(x) AS INT64)),
    SAFE_CAST(clean_str(x) AS TIMESTAMP)
  )
);
-- Radio access technology aliases -> canonical value.
CREATE TEMP FUNCTION norm_rat(x STRING) AS (
  CASE REGEXP_REPLACE(UPPER(clean_str(x)), r'[\s_]', '-')
    WHEN '2G' THEN '2G' WHEN 'GSM' THEN '2G'
    WHEN '3G' THEN '3G' WHEN 'UMTS' THEN '3G' WHEN 'WCDMA' THEN '3G'
    WHEN '4G' THEN '4G' WHEN 'LTE' THEN '4G'
    WHEN '5G' THEN '5G' WHEN 'NR' THEN '5G' WHEN '5G-NSA' THEN '5G'
    WHEN 'NB-IOT' THEN 'NB-IOT' WHEN 'NBIOT' THEN 'NB-IOT'
    WHEN 'LTE-M' THEN 'LTE-M' WHEN 'CAT-M1' THEN 'LTE-M'
    ELSE NULL
  END
);
CREATE TEMP FUNCTION salted_hash(salt STRING, x STRING) AS (
  IF(x IS NULL, NULL, TO_HEX(SHA256(CONCAT(salt, '|', x))))
);

BEGIN TRANSACTION;

-- ---- 1. Conform: rename + type + normalise ----------------------------------
CREATE TEMP TABLE conformed AS
SELECT
  UPPER(clean_str(device_id))                             AS device_id,
  parse_event_ts(time_stamp)                              AS event_ts,
  REGEXP_REPLACE(clean_str(imei), r'\D', '')              AS imei_digits,
  clean_str(imsi)                                         AS imsi,
  REGEXP_REPLACE(clean_str(msisdn), r'[^\d+]', '')        AS msisdn,
  clean_str(mcc)                                          AS mcc,
  clean_str(mnc)                                          AS mnc,
  SAFE_CAST(clean_str(tac) AS INT64)                      AS tac,
  clean_str(cell_id)                                      AS cell_id,
  norm_rat(network_type)                                  AS network_type,
  clean_num(rsrp)                                         AS rsrp_dbm,
  clean_num(rsrq)                                         AS rsrq_db,
  clean_num(sinr)                                         AS sinr_db,
  clean_num(battery)                                      AS battery_pct,
  clean_num(temp_c)                                       AS temperature_c,
  SAFE_CAST(clean_str(bytes_up) AS INT64)                 AS bytes_up,
  SAFE_CAST(clean_str(bytes_down) AS INT64)               AS bytes_down,
  SAFE_CAST(clean_str(lat) AS FLOAT64)                    AS latitude,
  SAFE_CAST(clean_str(long) AS FLOAT64)                   AS longitude,
  REGEXP_REPLACE(clean_str(fw_version), r'^[vV]', '')     AS firmware_version,
  UPPER(REGEXP_REPLACE(clean_str(device_type), r'[\s-]+', '_')) AS device_type,
  UPPER(clean_str(event))                                 AS event_type,
  source_file,
  REGEXP_EXTRACT(source_file, r'part-([0-9T]+)\.csv$')    AS batch_id,
  ingested_at
FROM `${landing_table}`;

-- ---- 2. Validate + dedupe (rows lacking a key are rejected, not loaded) ------
CREATE TEMP TABLE cleaned AS
SELECT
  TO_HEX(SHA256(CONCAT(device_id, '|', FORMAT_TIMESTAMP('%FT%H:%M:%E3SZ', event_ts)))) AS event_id,
  device_id,
  event_ts,
  DATE(event_ts) AS event_date,
  IF(LENGTH(imei_digits) = 15, SUBSTR(imei_digits, 1, 8), NULL) AS imei_tac,
  salted_hash(@pii_salt, IF(LENGTH(imei_digits) = 15, imei_digits, NULL)) AS imei_hash,
  salted_hash(@pii_salt, imsi)   AS imsi_hash,
  salted_hash(@pii_salt, msisdn) AS msisdn_hash,
  mcc, mnc, tac, cell_id, network_type,
  rsrp_dbm, rsrq_db, sinr_db, battery_pct, temperature_c,
  bytes_up, bytes_down, latitude, longitude,
  firmware_version, device_type, event_type,
  dq_flags,
  ARRAY_LENGTH(dq_flags) = 0 AS is_valid,
  source_file, batch_id, ingested_at,
  CURRENT_TIMESTAMP() AS processed_at
FROM (
  SELECT
    *,
    ARRAY(
      SELECT flag FROM UNNEST([
        IF(rsrp_dbm NOT BETWEEN @rsrp_min AND @rsrp_max, 'rsrp_out_of_range', NULL),
        IF(rsrq_db NOT BETWEEN @rsrq_min AND @rsrq_max, 'rsrq_out_of_range', NULL),
        IF(sinr_db NOT BETWEEN @sinr_min AND @sinr_max, 'sinr_out_of_range', NULL),
        IF(battery_pct NOT BETWEEN @battery_min AND @battery_max, 'battery_out_of_range', NULL),
        IF(temperature_c NOT BETWEEN @temp_min AND @temp_max, 'temperature_out_of_range', NULL),
        IF(latitude NOT BETWEEN @lat_min AND @lat_max
           OR longitude NOT BETWEEN @lon_min AND @lon_max, 'outside_service_area', NULL),
        IF(network_type IS NULL, 'unknown_network_type', NULL)
      ]) AS flag
      WHERE flag IS NOT NULL
    ) AS dq_flags
  FROM conformed
  WHERE device_id IS NOT NULL AND event_ts IS NOT NULL
)
-- Exact and replayed duplicates: keep the most recently ingested copy.
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at DESC, source_file DESC) = 1;

SET affected_dates = (SELECT ARRAY_AGG(DISTINCT event_date) FROM cleaned);

-- ---- 3. Idempotent upsert ------------------------------------------------------
MERGE `${silver_table}` AS t
USING cleaned AS s
ON t.event_id = s.event_id
   AND t.event_date = s.event_date
   AND t.event_date IN UNNEST(affected_dates)   -- partition pruning (required filter)
WHEN MATCHED THEN UPDATE SET
  device_id = s.device_id, event_ts = s.event_ts, imei_tac = s.imei_tac,
  imei_hash = s.imei_hash, imsi_hash = s.imsi_hash, msisdn_hash = s.msisdn_hash,
  mcc = s.mcc, mnc = s.mnc, tac = s.tac, cell_id = s.cell_id, network_type = s.network_type,
  rsrp_dbm = s.rsrp_dbm, rsrq_db = s.rsrq_db, sinr_db = s.sinr_db,
  battery_pct = s.battery_pct, temperature_c = s.temperature_c,
  bytes_up = s.bytes_up, bytes_down = s.bytes_down,
  latitude = s.latitude, longitude = s.longitude,
  firmware_version = s.firmware_version, device_type = s.device_type, event_type = s.event_type,
  is_valid = s.is_valid, dq_flags = s.dq_flags,
  source_file = s.source_file, batch_id = s.batch_id,
  ingested_at = s.ingested_at, processed_at = s.processed_at
WHEN NOT MATCHED THEN INSERT (
  event_id, device_id, event_ts, event_date, imei_tac, imei_hash, imsi_hash, msisdn_hash,
  mcc, mnc, tac, cell_id, network_type, rsrp_dbm, rsrq_db, sinr_db, battery_pct,
  temperature_c, bytes_up, bytes_down, latitude, longitude, firmware_version, device_type,
  event_type, is_valid, dq_flags, source_file, batch_id, ingested_at, processed_at
) VALUES (
  s.event_id, s.device_id, s.event_ts, s.event_date, s.imei_tac, s.imei_hash, s.imsi_hash,
  s.msisdn_hash, s.mcc, s.mnc, s.tac, s.cell_id, s.network_type, s.rsrp_dbm, s.rsrq_db,
  s.sinr_db, s.battery_pct, s.temperature_c, s.bytes_up, s.bytes_down, s.latitude,
  s.longitude, s.firmware_version, s.device_type, s.event_type, s.is_valid, s.dq_flags,
  s.source_file, s.batch_id, s.ingested_at, s.processed_at
);

-- ---- 4. Advance the watermark in the same transaction (never moves backwards) ----
MERGE `${watermark_table}` AS w
USING (SELECT @watermark_key AS pipeline) AS k
ON w.pipeline = k.pipeline
WHEN MATCHED THEN UPDATE SET
  watermark_ts = GREATEST(w.watermark_ts, @new_watermark),
  last_batch_id = @last_batch_id,
  updated_by_run_id = @run_id,
  updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (pipeline, watermark_ts, last_batch_id, updated_by_run_id, updated_at)
  VALUES (@watermark_key, @new_watermark, @last_batch_id, @run_id, CURRENT_TIMESTAMP());

COMMIT TRANSACTION;

-- ---- 5. Run statistics (last statement = job result) ---------------------------
SELECT
  (SELECT COUNT(*) FROM conformed)                                       AS rows_in,
  (SELECT COUNT(*) FROM conformed
     WHERE device_id IS NULL OR event_ts IS NULL)                        AS rows_rejected,
  (SELECT COUNT(*) FROM cleaned)                                         AS rows_out,
  (SELECT COUNTIF(NOT is_valid) FROM cleaned)                            AS rows_invalid,
  (SELECT COUNT(*) FROM conformed WHERE device_id IS NOT NULL AND event_ts IS NOT NULL)
    - (SELECT COUNT(*) FROM cleaned)                                     AS rows_deduplicated,
  ARRAY(SELECT CAST(d AS STRING) FROM UNNEST(affected_dates) AS d ORDER BY d) AS affected_dates,
  ARRAY(SELECT DISTINCT batch_id FROM cleaned ORDER BY batch_id)         AS batch_ids;
