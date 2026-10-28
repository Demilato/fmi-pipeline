-- Daily table: one UTC day of observations from all stations, deduplicated and quality-flagged.
-- Destination: {daily_table} (written with WRITE_TRUNCATE, so re-running a day is safe).
-- Parameters: @day DATE
WITH day_rows AS (
  SELECT *
  FROM `{raw_table}`
  WHERE obs_time >= TIMESTAMP(@day)
    AND obs_time < TIMESTAMP(DATE_ADD(@day, INTERVAL 1 DAY))
),

-- Duplicates come from (1) consecutive 20-minute polling windows overlapping and
-- (2) at-least-once delivery Kafka -> BigQuery. Keep the latest fetched copy.
dedup AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY fmisid, obs_time
        ORDER BY fetched_at DESC, kafka_offset DESC
      ) AS rn
    FROM day_rows
  )
  WHERE rn = 1
),

with_previous AS (
  SELECT
    *,
    LAG(temperature) OVER station_time AS prev_temperature,
    LAG(obs_time) OVER station_time AS prev_obs_time
  FROM dedup
  WINDOW station_time AS (PARTITION BY fmisid ORDER BY obs_time)
),

flagged AS (
  SELECT
    *,
    -- Range checks: physically implausible values for Finland are flagged as possible outliers.
    IFNULL(temperature NOT BETWEEN -55 AND 40, FALSE) AS qc_temperature_range,
    IFNULL(humidity NOT BETWEEN 0 AND 100, FALSE) AS qc_humidity_range,
    IFNULL(pressure NOT BETWEEN 900 AND 1080, FALSE) AS qc_pressure_range,
    IFNULL(wind_speed NOT BETWEEN 0 AND 50 OR wind_gust NOT BETWEEN 0 AND 75, FALSE) AS qc_wind_range,
    IFNULL(dew_point > temperature + 1, FALSE) AS qc_dew_point_above_temperature,
    -- Spike check: temperature jumping more than 8 degC within 30 minutes.
    IFNULL(
      ABS(temperature - prev_temperature) > 8
        AND TIMESTAMP_DIFF(obs_time, prev_obs_time, MINUTE) <= 30,
      FALSE
    ) AS qc_temperature_spike,
    -- Missing data: number of core parameters without a value.
    (
      IF(temperature IS NULL, 1, 0) + IF(humidity IS NULL, 1, 0) + IF(dew_point IS NULL, 1, 0)
      + IF(pressure IS NULL, 1, 0) + IF(wind_speed IS NULL, 1, 0)
    ) AS missing_core_params
  FROM with_previous
)

SELECT
  fmisid,
  station_name,
  latitude,
  longitude,
  obs_time,
  temperature,
  dew_point,
  humidity,
  pressure,
  wind_speed,
  wind_gust,
  wind_direction,
  cloud_cover,
  precipitation_1h,
  missing_core_params,
  qc_temperature_range,
  qc_humidity_range,
  qc_pressure_range,
  qc_wind_range,
  qc_dew_point_above_temperature,
  qc_temperature_spike,
  NOT (
    qc_temperature_range OR qc_humidity_range OR qc_pressure_range OR qc_wind_range
    OR qc_dew_point_above_temperature OR qc_temperature_spike
  ) AS qc_ok,
  fetched_at,
  CURRENT_TIMESTAMP() AS processed_at
FROM flagged
