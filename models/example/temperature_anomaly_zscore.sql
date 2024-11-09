WITH stats AS (
    SELECT
        city_name AS location_name,
        AVG(temperature_celsius) AS avg_temperature,
        STDDEV(temperature_celsius) AS stddev_temperature,
        COUNT(temperature_celsius) AS record_count
    FROM {{ ref('staging_weather') }}
    GROUP BY city_name
),
zscore_calc AS (
    SELECT
        sw.*,
        stats.avg_temperature,
        stats.stddev_temperature,
        stats.record_count,
        -- Safely calculate Z-Score only if STDDEV is not NULL or zero
        CASE
            WHEN stats.stddev_temperature > 0 THEN
                (sw.temperature_celsius - stats.avg_temperature) / stats.stddev_temperature
            ELSE 0 -- Default to 0 if no variation
        END AS z_score,
        -- Determine data status
        CASE
            WHEN stats.record_count < 2 THEN 'Insufficient Data'
            WHEN stats.stddev_temperature = 0 THEN 'No Variation'
            ELSE 'Valid'
        END AS data_status
    FROM {{ ref('staging_weather') }} AS sw
    LEFT JOIN stats
    ON sw.city_name = stats.location_name
)
SELECT
    *,
    -- Categorize anomalies if data is valid
    CASE
        WHEN data_status = 'Valid' AND ABS(z_score) > 2 THEN 'Outlier'
        ELSE 'Normal'
    END AS temperature_outlier_status
FROM zscore_calc
