WITH raw_weather AS (
    SELECT
        location_name,
        region,
        country,
        latitude,
        longitude,
        timezone,
        TIMESTAMP(localtime) AS localtime,
        TIMESTAMP(last_updated) AS last_updated,
        temperature_c,
        COALESCE(temperature_f, (temperature_c * 9/5) + 32) AS temperature_f,
        humidity,
        wind_kph,
        wind_dir,
        pressure_mb,
        visibility_km,
        uv_index,
        air_quality_co,
        air_quality_pm2_5,
        air_quality_pm10,
        CASE
            WHEN air_quality_pm2_5 <= 12 THEN 'Good'
            WHEN air_quality_pm2_5 <= 35 THEN 'Moderate'
            WHEN air_quality_pm2_5 <= 55 THEN 'Unhealthy for Sensitive Groups'
            WHEN air_quality_pm2_5 <= 150 THEN 'Unhealthy'
            ELSE 'Hazardous'
        END AS air_quality_category,
        CASE
            WHEN is_day = 1 THEN 'Day'
            ELSE 'Night'
        END AS day_or_night,
        requested_location
    FROM {{ source('weather_data', 'data') }}
)
SELECT * FROM raw_weather
