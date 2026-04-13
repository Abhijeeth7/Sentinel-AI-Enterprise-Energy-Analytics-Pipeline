{{ config(materialized='table') }}

WITH base_data AS (
    SELECT
        -- Flattening the JSON
        json_data:device_id::STRING AS device_id,
        json_data:region::STRING AS region,
        -- Correctly parsing your MM-DD-YYYY format into a Date object
        TO_DATE(json_data:event_date::STRING, 'MM-DD-YYYY') AS event_date,
        json_data:kwh_usage::FLOAT AS kwh_usage,
        json_data:carbon_tax_rate::FLOAT AS carbon_tax_rate,
        json_data:operational_status::STRING AS status,
        ingested_at AS technical_ingestion_timestamp
    FROM {{ source('snowflake_raw', 'raw_energy_data') }}
)

SELECT
    *,
    -- Adding business logic: Calculating the actual cost
    ROUND(kwh_usage * carbon_tax_rate, 2) AS carbon_tax_cost
FROM base_data