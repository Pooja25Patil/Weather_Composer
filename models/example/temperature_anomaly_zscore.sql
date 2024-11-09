WITH stats AS (
    SELECT
        location_name,
        AVG(temperature_c) AS avg_temperature,
        STDDEV(temperature_c) AS stddev_temperature,
        COUNT(temperature_c) AS record_count
    FROM {{ ref('staging_weather') }}
    GROUP BY location_name
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
                (sw.temperature_c - stats.avg_temperature) / stats.stddev_temperature
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
    ON sw.location_name = stats.location_name
)
SELECT
    *,
    -- Categorize anomalies if data is valid
    CASE
        WHEN data_status = 'Valid' AND ABS(z_score) > 2 THEN 'Outlier'
        ELSE 'Normal'
    END AS temperature_outlier_status
FROM zscore_calc
