SELECT
    location_name,
    country,
    localtime,
    temperature_c,
    humidity,
    air_quality_pm2_5,
    -- Compute a composite severity index
    (temperature_c * 0.4) + (humidity * 0.3) + (air_quality_pm2_5 * 0.3) AS weather_severity_index
FROM {{ ref('staging_weather') }}
