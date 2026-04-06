{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'cid'
    )
}}

WITH base AS (
    SELECT *,
    REGEXP_REPLACE(cid, '[^0-9]', '')::int AS _loc_id
    FROM {{ source('staging','erp_loc_a101') }}
),
source_data AS (
    SELECT * 
    FROM base

    {% if is_incremental() %}
    WHERE _loc_id > (
        SELECT COALESCE(MAX(_loc_id), -999999999) 
        FROM {{ this }}
    )
    {% endif %}
)
SELECT * FROM source_data