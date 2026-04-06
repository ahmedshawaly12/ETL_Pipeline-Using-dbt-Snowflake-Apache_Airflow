{{
     config(
          materialized = 'incremental',
          incremental_strategy = 'merge',
          unique_key = 'order_number'
     )
}}




WITH base AS (
    SELECT
        sls_ord_num AS order_number,
        sls_prd_key AS product_key,
        sls_cust_id AS customer_id,
        sls_order_dt AS order_date,
        sls_ship_dt AS ship_date,
        sls_due_dt AS due_date,
        sls_sales AS sales_amount,
        sls_quantity AS quantity,
        sls_price AS price
    FROM {{ ref("slv_crm_sales_details") }} AS sls
    LEFT JOIN {{ ref("dim_products") }} AS prd
    ON sls.sls_prd_key = prd.product_key and prd.end_date IS NULL
    LEFT JOIN {{ ref("dim_customers") }} AS cust
    ON sls.sls_cust_id  = cust.customer_id and cust.dbt_valid_to IS NULL

    

)

SELECT *
FROM base
{% if is_incremental() %}
WHERE SUBSTRING(order_number, 3, LENGTH(order_number))::int > (
     SELECT COALESCE(MAX(SUBSTRING(order_number, 3, LENGTH(order_number))::int), -999999999999)
     FROM {{ this }}
)
{% endif %}