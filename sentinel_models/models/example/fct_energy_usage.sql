{{ config(materialized='table') }}

WITH stg_data AS (
    SELECT * FROM {{ ref('stg_energy_data') }}
)

SELECT
    -- The Glue: These keys connect to our Dimensions
    {{ dbt_utils.generate_surrogate_key(['device_id']) }} as device_key,
    {{ dbt_utils.generate_surrogate_key(['region']) }} as region_key,
    
    -- The Metrics: What we actually want to sum up
    event_date,
    kwh_usage,
    carbon_tax_cost,
    status
FROM stg_data