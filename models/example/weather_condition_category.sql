SELECT
    location.name AS location_name,
    location.country AS country,
    current.temp_c AS temperature_c,
    current.humidity AS humidity,
    -- Categorize weather conditions
    CASE
        WHEN current.temp_c > 30 AND current.humidity < 50 THEN 'Hot and Dry'
        WHEN current.temp_c > 30 AND current.humidity >= 50 THEN 'Hot and Humid'
        WHEN current.temp_c BETWEEN 15 AND 30 THEN 'Mild'
        ELSE 'Cold'
    END AS weather_category
FROM {{ ref('staging_weather') }};
