WITH weather_summary AS (
    SELECT
        city_name AS location_name,
        country AS country,
        AVG(temperature_celsius) AS avg_temperature_c,
        AVG(temperature_fahrenheit) AS avg_temperature_f,
        AVG(humidity) AS avg_humidity,
        AVG(air_quality_pm2_5) AS avg_pm2_5,
        AVG(air_quality_pm10) AS avg_pm10,
        MAX(uv_index) AS max_uv_index,
        COUNT(*) AS record_count,
        MAX(localtime) AS latest_record_time
    FROM {{ ref('staging_weather') }}
    GROUP BY city_name, country
)
SELECT * FROM weather_summary 