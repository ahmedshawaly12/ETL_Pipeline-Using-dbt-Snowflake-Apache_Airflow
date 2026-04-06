{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'cid'
    )
}}

SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
         WHEN TRIM(cntry) = 'DE' THEN 'Germany'
         WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
         ELSE TRIM(cntry)
    END AS cntry
FROM {{ ref("brz_erp_loc_a101") }}

{% if is_incremental() %}

WHERE REGEXP_REPLACE(cid,'[^0-9]', '')::int > (
    SELECT COALESCE(MAX(REGEXP_REPLACE(cid,'[^0-9]', '')::int), -9999999999)
    FROM {{ this }}
)
{% endif %}