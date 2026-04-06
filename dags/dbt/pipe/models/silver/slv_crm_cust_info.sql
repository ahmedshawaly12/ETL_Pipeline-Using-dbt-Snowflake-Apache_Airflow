{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'cst_id'
    )
}}


WITH base AS (
    SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) cst_lastname,

    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
         WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
         ELSE 'n/a' 
    END  AS cst_material_status,

    CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
         WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'n/a' 
    END AS cst_gndr,

    cst_create_date,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as row_num
    FROM {{ ref("brz_crm_cust_info") }}
    WHERE cst_id IS NOT NULL 
),
source_data AS (
    SELECT 
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_material_status,
        cst_gndr,
        cst_create_date
    FROM base
    WHERE row_num = 1
)

SELECT * 
FROM source_data

{% if is_incremental() %}
WHERE cst_create_date > (
    SELECT COALESCE(MAX(cst_create_date), '1900-01-01'::date)
    FROM {{ this }}
) - INTERVAL '1 day'
{% endif %}

