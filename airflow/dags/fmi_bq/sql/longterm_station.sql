-- Long-term table of one station: existing history + the station's rows from the new daily table.
-- Destination: {longterm_table} (WRITE_TRUNCATE of the merged result). Parameters: @fmisid INT64
-- The sandbox has no DML (INSERT/MERGE), so the "upsert" is done by rewriting the small table.
-- If a day is processed again, its newer rows replace the older copies.
WITH combined AS (
  SELECT
    obs_time, fmisid, station_name, temperature, humidity, dew_point, pressure,
    wind_speed, wind_gust, precipitation_1h, qc_ok, processed_at
  FROM `{daily_table}`
  WHERE fmisid = @fmisid

  UNION ALL

  SELECT
    obs_time, fmisid, station_name, temperature, humidity, dew_point, pressure,
    wind_speed, wind_gust, precipitation_1h, qc_ok, processed_at
  FROM `{longterm_table}`
)

SELECT * EXCEPT (rn)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY obs_time ORDER BY processed_at DESC) AS rn
  FROM combined
)
WHERE rn = 1
