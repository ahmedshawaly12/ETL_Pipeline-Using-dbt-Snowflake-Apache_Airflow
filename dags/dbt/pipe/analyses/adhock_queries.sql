SELECT * FROM {{ref("slv_crm_cust_info")}}

--------------------------------------------------
-- brz_crm_cust_info
--------------------------------------------------

-- check for null or duplicates in primary key
SELECT cst_id , COUNT(*)
FROM {{ ref("brz_crm_cust_info") }}
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_id IS NULL

WITH base AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as row_num
    FROM {{ ref("brz_crm_cust_info") }}
    WHERE cst_id IS NOT NULL 
)

-- Check for unwanted spaces.
SELECT * FROM {{ ref('brz_crm_cust_info') }}
WHERE cst_lastname != TRIM(cst_lastname)
{# WHERE cst_fistname != TRIM(cst_fistname) #}
{# WHERE cst_key != TRIM(cst_key) #}


-- Data Standardization & Consistency
SELECT  cst_material_status, COUNT(*)
FROM {{ ref("brz_crm_cust_info") }}
GROUP BY  cst_material_status

SELECT  cst_gndr, COUNT(*)
FROM {{ ref("brz_crm_cust_info") }}
GROUP BY  cst_gndr


SELECT * FROM {{ ref('slv_crm_cust_info') }}


--------------------------------------------------
-- brz_crm_prd_info
--------------------------------------------------

-- Check for nulls or duplicates in primary key 
SELECT prd_id , COUNT(*)
FROM {{ ref("brz_crm_prd_info") }}
GROUP BY prd_id
HAVING COUNT(*) > 1 or prd_id IS NULL



-- Check for unwanted space
SELECT *
FROM {{ ref("brz_crm_prd_info") }}
WHERE prd_nm != TRIM(prd_nm)
-- WHERE prd_key != TRIM(prd_key)


-- CHeck for Nulls or nigative values in cost
SELECT * 
FROM {{ ref("brz_crm_prd_info") }}
WHERE prd_cost  <= 0 OR prd_cost IS NULL


-- Data Standardization & Consistency
SELECT prd_line, COUNT(*)   
FROM {{ ref("brz_crm_prd_info") }}
GROUP BY prd_line;


-- Check Invalid dates
SELECT *
FROM {{ ref('brz_crm_prd_info') }}
WHERE prd_end_dt < prd_start_dt


SELECT * ,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 as test_dt 
FROM {{ ref("brz_crm_prd_info") }}
ORDER BY prd_key , prd_start_dt




SELECT * 
FROM {{ ref("slv_crm_prd_info") }}


--------------------------------------------------
-- brz_crm_sales_details
--------------------------------------------------

-- Check for Nulls or Duplicates in the primary key



-- Check for unwanted spaces
SELECT * 
FROM {{ ref("brz_crm_sales_details") }}
WHERE sls_prd_key != TRIM(sls_prd_key)

-- Check for invalid dates
SELECT *
FROM  {{ ref("brz_crm_sales_details") }}
WHERE sls_order_dt <= 0 OR sls_order_dt IS NULL OR LENGTH(sls_order_dt::varchar) != 8

SELECT *
FROM  {{ ref("brz_crm_sales_details") }}
WHERE sls_ship_dt <= 0 OR sls_ship_dt IS NULL OR LENGTH(sls_ship_dt::varchar) != 8

SELECT *
FROM  {{ ref("brz_crm_sales_details") }}
WHERE sls_due_dt <= 0 OR sls_due_dt IS NULL OR LENGTH(sls_due_dt::varchar) != 8

SELECT *
FROM {{ ref("brz_crm_sales_details") }}
WHERE sls_order_dt > sls_ship_dt 
    OR sls_order_dt > sls_due_dt


-- Check data consistency sales = quantity * price
SELECT sls_sales, sls_quantity, sls_price
FROM {{ ref("brz_crm_sales_details") }}
WHERE sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL
    OR sls_quantity IS NULL
    OR sls_price IS NULL 
    OR sls_sales <= 0
    OR sls_quantity <= 0
    OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


-- Data Integrity between foreign keys and primary keys
SELECT  * FROM {{ ref("brz_crm_sales_details") }} 
WHERE sls_prd_key NOT IN 
(SELECT  SUBSTRING(prd_key, 7, LENGTH(prd_key)) sls_prd_key FROM {{ ref("slv_crm_prd_info") }})


SELECT  * FROM {{ ref("brz_crm_sales_details") }}
WHERE sls_cust_id NOT IN (
SELECT  cst_id FROM {{ ref("slv_crm_cust_info") }})




SELECT * FROM {{ ref("slv_crm_sales_details") }}

--------------------------------------------------
--                     ERP
--------------------------------------------------


--------------------------------------------------
-- brz_erp_cust_az12
--------------------------------------------------

-- Check for nulls or duplicates in the primary key
SELECT cid, COUNT(*)
FROM {{ ref("brz_erp_cust_az12") }}
GROUP BY cid
HAVING COUNT(*) > 1 OR cid IS NULL

SELECT * 
FROM {{ ref("brz_erp_cust_az12") }}
WHERE cid != TRIM(cid)


-- Data Standardization & Consistency
SELECT distinct TRIM(gen)
FROM {{ ref("brz_erp_cust_az12") }}


-- Check for invalid dates
SELECT MIN(bdate), MAX(bdate) FROM {{ ref("brz_erp_cust_az12") }}

SELECT *
FROM {{ ref("brz_erp_cust_az12") }} 
WHERE bdate < '1924-01-01'::DATE OR bdate > CURRENT_DATE

-- Data Integrity between foreign keys and primary keys
WITH source_data AS(

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
)
SELECT *
FROM source_data
WHERE cid NOT IN (
    SELECT cst_key FROM {{ ref("slv_crm_cust_info") }}
)


--------------------------------------------------
-- brz_erp_cust_az12
--------------------------------------------------

-- Check for duplicated or Nulls in the primary key
SELECT cid, COUNT(*)
FROM  {{ ref("brz_erp_loc_a101") }}
GROUP BY cid
HAVING COUNT(*) > 1 OR cid IS NULL OR cid != TRIM(cid)


-- Data integrity
SELECT REPLACE(cid, '-', '') AS cid, cntry
FROM {{ ref("brz_erp_loc_a101") }}
WHERE REPLACE(cid, '-', '') NOT IN (
    SELECT cst_key FROM {{ ref("slv_crm_cust_info") }}
)

-- Data Standardization  & Consistency
SELECT DISTINCT TRIM(cntry) 
FROM {{ ref("brz_erp_loc_a101") }}





--------------------------------------------------
-- brz_erp_px_cat_g1v2
--------------------------------------------------

-- Check for Nulls or Duplicates in the primary key
SELECT id, COUNT(*)
FROM {{ ref("brz_erp_px_cat_g1v2") }}
GROUP BY id
HAVING COUNT(*) > 1 OR id IS NULL

-- Data Integrity between the primary key and the foreign key



SELECT * FROM {{ ref("brz_erp_px_cat_g1v2") }}
WHERE id NOT IN (
SELECT cat_id FROM {{ ref("slv_crm_prd_info") }}
)


-- Check for unwanted spaces
SELECT *
FROM {{ ref("brz_erp_px_cat_g1v2") }}
WHERE cat != TRIM(cat)
OR subcat != TRIM(subcat)
OR maintenance != TRIM(maintenance)


-- Data Standardization & Consistency
SELECT DISTINCT maintenance
FROM {{ ref("brz_erp_px_cat_g1v2") }}


