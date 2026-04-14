{{ config(materialized='table') }}

WITH fact_usage AS (
    SELECT * FROM {{ ref('fct_energy_usage') }}
),

dim_regions AS (
    SELECT * FROM {{ ref('dim_regions') }}
)

SELECT
    r.region_name,
    r.manager_name,
    r.manager_email,
    -- Time Intelligence
    DATE_TRUNC('month', f.event_date) AS reporting_month,
    
    -- Aggregated Metrics
    COUNT(*) AS total_readings,
    ROUND(SUM(f.kwh_usage), 2) AS total_kwh_consumed,
    ROUND(SUM(f.carbon_tax_cost), 2) AS total_tax_liability,
    
    -- Goal Tracking Logic (Business Intelligence)
    r.carbon_goal_kwh,
    ROUND((SUM(f.kwh_usage) / r.carbon_goal_kwh) * 100, 2) AS goal_attainment_pct,
    
    -- Status Flag for Power BI Conditional Formatting
    CASE 
        WHEN (SUM(f.kwh_usage) / r.carbon_goal_kwh) > 0.90 THEN 'CRITICAL'
        WHEN (SUM(f.kwh_usage) / r.carbon_goal_kwh) > 0.75 THEN 'WARNING'
        ELSE 'STABLE'
    END AS regional_health_status

FROM fact_usage f
JOIN dim_regions r ON f.region_key = r.region_key
GROUP BY 1, 2, 3, 4, 8