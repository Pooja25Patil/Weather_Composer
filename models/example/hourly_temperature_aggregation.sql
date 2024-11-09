SELECT
    location_name,
    country,
    -- Truncate timestamps to the nearest hour
    TIMESTAMP_TRUNC(localtime, HOUR) AS hour,
    -- Aggregate temperature data
    AVG(temperature_c) AS avg_hourly_temperature,
    MAX(temperature_c) AS max_hourly_temperature,
    MIN(temperature_c) AS min_hourly_temperature
FROM {{ ref('staging_weather') }}
GROUP BY location_name, country, hour
