SELECT
    location_name,
    country,
    temperature_c,
    humidity,
    -- Categorize weather conditions
    CASE
        WHEN temperature_c > 30 AND humidity < 50 THEN 'Hot and Dry'
        WHEN temperature_c > 30 AND humidity >= 50 THEN 'Hot and Humid'
        WHEN temperature_c BETWEEN 15 AND 30 THEN 'Mild'
        ELSE 'Cold'
    END AS weather_category
FROM {{ ref('staging_weather') }}
