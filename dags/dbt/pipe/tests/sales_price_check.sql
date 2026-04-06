SELECT *
FROM {{ ref("slv_crm_sales_details") }}
WHERE sls_sales != sls_quantity * sls_price