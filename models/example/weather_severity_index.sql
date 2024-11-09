SELECT
    city_name AS location_name,
    country AS country,
    localtime AS localtime,
    temperature_celsius AS temperature_c,
    humidity AS humidity,
    air_quality_pm2_5 AS air_quality_pm2_5,
    -- Compute a composite severity index
    (temperature_celsius * 0.4) + (humidity * 0.3) + (air_quality_pm2_5 * 0.3) AS weather_severity_index
FROM {{ ref('staging_weather') }} 