{{
    config(
        materialized='incremental',
        incremental_strategy = 'merge',
        unique_key = 'sls_ord_num'
    )
}}

WITH base AS (
    SELECT *,
    SUBSTRING(sls_ord_num, 3, LENGTH(sls_ord_num))::int as _order_number
   FROM {{ source('staging', 'crm_sales_details') }}
),
source_data AS (
    SELECT *
    FROM base 

    {% if is_incremental() %}

    WHERE _order_number > (
        SELECT COALESCE(MAX(_order_number), -999999999)
        FROM {{ this }}
    )

    {% endif %}
)

SELECT * FROM source_data