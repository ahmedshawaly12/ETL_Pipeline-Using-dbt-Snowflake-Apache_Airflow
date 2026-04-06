SELECT *
FROM {{ ref("slv_crm_prd_info") }}
WHERE prd_end_dt < prd_start_dt