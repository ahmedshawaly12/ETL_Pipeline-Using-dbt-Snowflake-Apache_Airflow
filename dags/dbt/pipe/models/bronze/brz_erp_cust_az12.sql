{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'cid'
) 
}}

WITH base AS (
    SELECT *,
    REGEXP_REPLACE(cid,'[^0-9]', '')::int AS _cust_id
    FROM {{ source("staging", "erp_cust_az12") }}
),
source_data AS (
    SELECT * 
    FROM base

    {% if is_incremental() %}
    WHERE _cust_id > (
        SELECT COALESCE(MAX(_cust_id), -9999999999)
        FROM {{ this }}
    )
    {% endif %}

)
SELECT * FROM source_data