{{
    config(
        materialized='incremental',
        incremental_strategy = 'merge',
        unique_key = 'prd_id'

    )
}}

WITH source_data AS (
    SELECT * 
    FROM {{ source('staging', 'crm_prd_info') }}

    {% if is_incremental() %}

    WHERE prd_start_dt > (
        SELECT COALESCE(MAX(prd_start_dt), '1900-01-01'::date)
        FROM {{ this }}
    ) - INTERVAL '1 day'

    {% endif %}
)

SELECT * FROM source_data