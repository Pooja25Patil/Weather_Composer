SELECT
    location_name,
    country,
    localtime,
    temperature_c,
    -- Compute a rolling average of the last 5 readings
    AVG(temperature_c) OVER (
        PARTITION BY location_name
        ORDER BY localtime
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS rolling_5_reading_avg_temperature
FROM {{ ref('staging_weather') }}
