SELECT
    location.name AS location_name,
    location.country AS country,
    TIMESTAMP(location.localtime) AS localtime,
    current.temp_c AS temperature_c,
    current.humidity AS humidity,
    current.air_quality.pm2_5 AS air_quality_pm2_5,
    -- Compute a composite severity index
    (current.temp_c * 0.4) + (current.humidity * 0.3) + (current.air_quality.pm2_5 * 0.3) AS weather_severity_index
FROM {{ ref('staging_weather') }};
