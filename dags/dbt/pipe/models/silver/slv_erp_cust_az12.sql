{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'cid'
    )
}}


SELECT 
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
        ELSE cid
    END AS cid,
    CASE WHEN bdate > CURRENT_DATE - INTERVAL '5 year' THEN NULL
        ELSE  bdate
    END As bdate,
    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')  THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM {{ ref("brz_erp_cust_az12") }}

{% if is_incremental() %}
WHERE REGEXP_REPLACE(cid,'[^0-9]', '')::int > (
    SELECT COALESCE(MAX(REGEXP_REPLACE(cid,'[^0-9]', '')::int), -9999999999)
    FROM {{ this }}
)
{% endif %}


