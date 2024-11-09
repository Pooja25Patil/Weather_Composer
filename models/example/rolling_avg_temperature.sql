SELECT
    city_name AS location_name,
    country AS country,
    localtime AS localtime,
    temperature_celsius AS temperature_c,
    -- Compute a rolling average of the last 5 readings
    AVG(temperature_celsius) OVER (
        PARTITION BY city_name
        ORDER BY TIMESTAMP(localtime)
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS rolling_5_reading_avg_temperature
FROM {{ ref('staging_weather') }} 