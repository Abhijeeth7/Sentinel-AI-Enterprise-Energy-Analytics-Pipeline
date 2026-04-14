{{ config(materialized='table') }}

SELECT
    -- Generating a unique hash key for the region
    {{ dbt_utils.generate_surrogate_key(['region_name']) }} as region_key,
    region_name,
    manager_name,
    manager_email,
    carbon_goal_kwh
FROM {{ ref('dim_regions_mapping') }}