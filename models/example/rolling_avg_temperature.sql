SELECT
    location.name AS location_name,
    location.country AS country,
    TIMESTAMP(location.localtime) AS localtime,
    current.temp_c AS temperature_c,
    -- Compute a rolling average of the last 5 readings
    AVG(current.temp_c) OVER (
        PARTITION BY location.name
        ORDER BY TIMESTAMP(location.localtime)
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS rolling_5_reading_avg_temperature
FROM {{ ref('staging_weather') }};
