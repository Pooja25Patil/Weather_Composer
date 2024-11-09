SELECT
    location.name AS location_name,
    location.country AS country,
    -- Truncate timestamps to the nearest hour
    TIMESTAMP_TRUNC(TIMESTAMP(location.localtime), HOUR) AS hour,
    -- Aggregate temperature data
    AVG(current.temp_c) AS avg_hourly_temperature,
    MAX(current.temp_c) AS max_hourly_temperature,
    MIN(current.temp_c) AS min_hourly_temperature
FROM {{ ref('staging_weather') }}
GROUP BY location.name, location.country, hour;
