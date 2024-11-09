SELECT
    city_name AS location_name,
    country AS country,
    -- Truncate timestamps to the nearest hour
    TIMESTAMP_TRUNC(TIMESTAMP(localtime), HOUR) AS hour,
    -- Aggregate temperature data
    AVG(temperature_celsius) AS avg_hourly_temperature,
    MAX(temperature_celsius) AS max_hourly_temperature,
    MIN(temperature_celsius) AS min_hourly_temperature
FROM {{ ref('staging_weather') }}
GROUP BY city_name, country, hour 