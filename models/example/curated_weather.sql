WITH weather_summary AS (
    SELECT
        location.name AS location_name,
        location.country AS country,
        AVG(current.temp_c) AS avg_temperature_c,
        AVG(current.temp_f) AS avg_temperature_f,
        AVG(current.humidity) AS avg_humidity,
        AVG(current.air_quality.pm2_5) AS avg_pm2_5,
        AVG(current.air_quality.pm10) AS avg_pm10,
        MAX(current.uv) AS max_uv_index,
        COUNT(*) AS record_count,
        MAX(TIMESTAMP(location.localtime)) AS latest_record_time
    FROM {{ ref('staging_weather') }}
    GROUP BY location.name, location.country
)
SELECT * FROM weather_summary;
