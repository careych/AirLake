-- Virtual dataset SELECT only. NOT EXECUTED. Historical, explicitly bounded UTC window.
SELECT f.location_id, d.name AS station_name, f.hour_start AS hour_start_utc,
       f.pm25_avg, f.aqi, 'µg/m³' AS units,
       'simplified_hourly_mean_mvp' AS aqi_method,
       date_diff('second', f.hour_start + INTERVAL '1' HOUR, current_timestamp) / 3600.0 AS age_hours,
       CASE WHEN f.hour_start + INTERVAL '1' HOUR > current_timestamp THEN 'FUTURE'
            WHEN date_diff('second', f.hour_start + INTERVAL '1' HOUR, current_timestamp) > 86400
            THEN 'STALE (>24h)' ELSE 'RECENT (<=24h)' END AS freshness,
       'Historical July 2025 Gold; NOT a live air-quality feed' AS dataset_label
FROM "AwsDataCatalog"."airlake_it2_20261106_f3b3b3e0677d4a6790e4a54faf68e4aa_gold"."pm25_aqi_hourly"
FOR VERSION AS OF 1174385021871906125 AS f
LEFT JOIN "AwsDataCatalog"."airlake_it2_20261106_f3b3b3e0677d4a6790e4a54faf68e4aa_gold"."dim_location"
FOR VERSION AS OF 4323462633891640540 AS d ON f.location_id = d.location_id
WHERE f.hour_start >= TIMESTAMP '2025-07-03 15:00:00 UTC'
  AND f.hour_start < TIMESTAMP '2025-07-04 14:00:00 UTC'
  AND f.location_id IN (4946812, 4946813, 6123215)
