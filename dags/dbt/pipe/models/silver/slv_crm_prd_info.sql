

WITH source_data AS (
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,   
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost ,
        CASE UPPER(TRIM(prd_line)) 
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,

        prd_start_dt::date AS prd_start_dt,

        LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS prd_end_dt

    FROM {{ ref('brz_crm_prd_info') }}
    ORDER BY prd_key , prd_start_dt

)
SELECT * 
FROM source_data

