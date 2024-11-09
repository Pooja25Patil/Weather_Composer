SELECT
    city_name AS location_name,
    country AS country,
    temperature_celsius AS temperature_c,
    humidity AS humidity,
    -- Categorize weather conditions
    CASE
        WHEN temperature_celsius > 30 AND humidity < 50 THEN 'Hot and Dry'
        WHEN temperature_celsius > 30 AND humidity >= 50 THEN 'Hot and Humid'
        WHEN temperature_celsius BETWEEN 15 AND 30 THEN 'Mild'
        ELSE 'Cold'
    END AS weather_category
FROM {{ ref('staging_weather') }} 