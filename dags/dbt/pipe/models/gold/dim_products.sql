{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='product_id'   
    )
}}

WITH  base AS (
    SELECT 
        prd.prd_id            AS product_id,
        prd.prd_key           AS product_key,
        prd.prd_nm            AS product_name,
        prd.cat_id            AS category_id,
        cat.cat               AS category,
        cat.subcat            AS subcategory,
        cat.maintenance       AS maintenance,

        prd.prd_cost          AS cost,
        prd.prd_line          AS product_line,

        prd.prd_start_dt      AS start_date,
        prd.prd_end_dt        AS end_date


    FROM {{ ref("slv_crm_prd_info") }} AS prd
    LEFT JOIN {{ ref("slv_erp_px_cat_g1v2") }} AS cat 
    ON prd.cat_id = cat.id 
    
)

SELECT * 
FROM base

{% if is_incremental() %}

WHERE start_date > (
    SELECT COALESCE(MAX(start_date), '1900-01-01'::date) 
    FROM {{ this }}
)

{% endif %}
