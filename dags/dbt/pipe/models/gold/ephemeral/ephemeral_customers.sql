{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_id'
    )
}}

WITH base AS (
    SELECT
        ci.cst_id                               AS customer_id,
        ci.cst_key                              AS customer_number,
        ci.cst_firstname                        AS first_name,
        ci.cst_lastname                         AS last_name,

        CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
             ELSE COALESCE(erp_ci.gen, 'n/a')
        END                                     AS gender,

        ci.cst_material_status                  AS marital_status,
        erp_loc.cntry                           AS country,
        erp_ci.bdate                            AS birthdate,
        ci.cst_create_date::timestamp           AS create_date

    FROM {{ ref("slv_crm_cust_info") }} AS ci
    LEFT JOIN {{ ref("slv_erp_cust_az12") }} AS erp_ci
        ON ci.cst_key = erp_ci.cid
    LEFT JOIN {{ ref("slv_erp_loc_a101") }} AS erp_loc
        ON ci.cst_key = erp_loc.cid
)

SELECT *
FROM base

{% if is_incremental() %}
WHERE create_date > (
    SELECT COALESCE(MAX(create_date), '1900-01-01'::date)
    FROM {{ this }}
)
{% endif %}