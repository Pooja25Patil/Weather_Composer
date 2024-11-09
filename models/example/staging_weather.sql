WITH raw_weather AS (
    SELECT
        location.name AS city_name,
        location.region,
        location.country,
        location.lat AS latitude,
        location.lon AS longitude,
        location.tz_id AS timezone,
        TIMESTAMP(location.localtime) AS localtime,
        TIMESTAMP(`current`.last_updated) AS last_updated,
        `current`.temp_c AS temperature_celsius,
        COALESCE(`current`.temp_f, (`current`.temp_c * 9/5) + 32) AS temperature_fahrenheit,
        `current`.humidity,
        `current`.wind_kph AS wind_speed_kph,
        `current`.wind_dir AS wind_direction,
        `current`.pressure_mb AS pressure_millibars,
        `current`.vis_km AS visibility_km,
        `current`.uv AS uv_index,
        `current`.air_quality.co AS air_quality_co,
        `current`.air_quality.pm2_5 AS air_quality_pm2_5,
        `current`.air_quality.pm10 AS air_quality_pm10,
        CASE
            WHEN `current`.air_quality.pm2_5 <= 12 THEN 'Good'
            WHEN `current`.air_quality.pm2_5 <= 35 THEN 'Moderate'
            WHEN `current`.air_quality.pm2_5 <= 55 THEN 'Unhealthy for Sensitive Groups'
            WHEN `current`.air_quality.pm2_5 <= 150 THEN 'Unhealthy'
            ELSE 'Hazardous'
        END AS air_quality_category,
        CASE
            WHEN `current`.is_day = 1 THEN 'Day'
            ELSE 'Night'
        END AS day_or_night,
        requested_location,
        fetched_at
    FROM {{ source('weather_data', 'data') }}
)
SELECT * FROM raw_weather
