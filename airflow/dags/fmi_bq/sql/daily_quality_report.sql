-- Data-quality report for one day: one row per station.
-- Destination: {quality_table} (WRITE_TRUNCATE). Parameters: @day DATE
-- Completeness assumes FMI's 10-minute observation interval (144 observations per day).
WITH raw_counts AS (
  SELECT fmisid, COUNT(*) AS raw_rows
  FROM `{raw_table}`
  WHERE obs_time >= TIMESTAMP(@day)
    AND obs_time < TIMESTAMP(DATE_ADD(@day, INTERVAL 1 DAY))
  GROUP BY fmisid
),

max_gaps AS (
  SELECT fmisid, MAX(gap_minutes) AS max_gap_minutes
  FROM (
    SELECT
      fmisid,
      TIMESTAMP_DIFF(obs_time, LAG(obs_time) OVER (PARTITION BY fmisid ORDER BY obs_time), MINUTE)
        AS gap_minutes
    FROM `{daily_table}`
  )
  GROUP BY fmisid
),

station_stats AS (
  SELECT
    fmisid,
    ANY_VALUE(station_name) AS station_name,
    COUNT(*) AS rows_after_dedup,
    MIN(obs_time) AS first_obs,
    MAX(obs_time) AS last_obs,
    COUNTIF(temperature IS NULL) AS missing_temperature,
    COUNTIF(humidity IS NULL) AS missing_humidity,
    COUNTIF(pressure IS NULL) AS missing_pressure,
    COUNTIF(wind_speed IS NULL) AS missing_wind_speed,
    COUNTIF(qc_temperature_range) AS flag_temperature_range,
    COUNTIF(qc_humidity_range) AS flag_humidity_range,
    COUNTIF(qc_pressure_range) AS flag_pressure_range,
    COUNTIF(qc_wind_range) AS flag_wind_range,
    COUNTIF(qc_dew_point_above_temperature) AS flag_dew_point,
    COUNTIF(qc_temperature_spike) AS flag_temperature_spike,
    COUNTIF(NOT qc_ok) AS rows_flagged,
    ROUND(AVG(temperature), 2) AS avg_temperature,
    MIN(temperature) AS min_temperature,
    MAX(temperature) AS max_temperature,
    ROUND(AVG(humidity), 1) AS avg_humidity
  FROM `{daily_table}`
  GROUP BY fmisid
)

SELECT
  @day AS report_day,
  s.fmisid,
  s.station_name,
  r.raw_rows,
  s.rows_after_dedup,
  r.raw_rows - s.rows_after_dedup AS duplicates_removed,
  ROUND(100 * s.rows_after_dedup / 144, 1) AS completeness_pct,
  s.first_obs,
  s.last_obs,
  g.max_gap_minutes,
  s.missing_temperature,
  s.missing_humidity,
  s.missing_pressure,
  s.missing_wind_speed,
  s.flag_temperature_range,
  s.flag_humidity_range,
  s.flag_pressure_range,
  s.flag_wind_range,
  s.flag_dew_point,
  s.flag_temperature_spike,
  s.rows_flagged,
  s.avg_temperature,
  s.min_temperature,
  s.max_temperature,
  s.avg_humidity,
  CURRENT_TIMESTAMP() AS processed_at
FROM station_stats AS s
LEFT JOIN raw_counts AS r ON r.fmisid = s.fmisid
LEFT JOIN max_gaps AS g ON g.fmisid = s.fmisid
