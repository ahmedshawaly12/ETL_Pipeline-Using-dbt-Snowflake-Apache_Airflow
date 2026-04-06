{{
     config(
          materialized = 'incremental',
          incremental_strategy = 'merge',
          unique_key = 'sls_ord_num'
     )
}}


WITH source_data AS (
     SELECT 
     sls_ord_num,
     sls_prd_key,
     sls_cust_id,

     CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::VARCHAR) != 8 THEN  NULL
          ELSE sls_order_dt::VARCHAR::DATE
     END AS sls_order_dt,

     CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_order_dt::VARCHAR) != 8 THEN NULL
          ELSE sls_ship_dt::VARCHAR::DATE
     END AS sls_ship_dt,

     CASE WHEN sls_due_dt = 0 OR LENGTH(sls_order_dt::VARCHAR) != 8 THEN NULL
          ELSE sls_due_dt::VARCHAR::DATE
     END AS sls_due_dt,

     CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
               THEN  sls_quantity * ABS(sls_price)
          ELSE sls_sales
     END AS sls_sales,

     sls_quantity,

     CASE WHEN sls_price <= 0 OR sls_price IS NULL 
               THEN  sls_sales / COALESCE(sls_quantity, 0)
          ELSE sls_price
     END AS sls_price

     FROM {{ ref("brz_crm_sales_details") }}
)

SELECT * 
FROM source_data

{% if is_incremental() %}
WHERE SUBSTRING(sls_ord_num, 3, LENGTH(sls_ord_num))::int > (
     SELECT COALESCE(MAX(SUBSTRING(sls_ord_num, 3, LENGTH(sls_ord_num))::int), -999999999999)
     FROM {{ this }}
)
{% endif %}