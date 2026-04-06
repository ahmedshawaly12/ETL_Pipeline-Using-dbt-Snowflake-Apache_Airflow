{{ 
    config(
        materialized='incremental',
        unique_key=['cst_key', 'cst_create_date'],
        incremental_strategy='merge'
    )
 }}

 WITH source_data AS (
    SELECT * 
    FROM {{ source('staging', 'crm_cust_info') }}

    {% if is_incremental() %}
    WHERE cst_create_date > (
        SELECT COALESCE(MAX(cst_create_date), '1900-01-01'::date)
        FROM {{ this }}
    ) - INTERVAL '1 day'
    {% endif %}
 )

 SELECT * FROM source_data