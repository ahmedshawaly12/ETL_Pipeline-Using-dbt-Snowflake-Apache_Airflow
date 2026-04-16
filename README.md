# Sales Data Pipeline — ERP & CRM to Data Warehouse

A data pipeline that ingests raw data from ERP and CRM source systems, transforms it through a Medallion Architecture (Bronze → Silver → Gold), and delivers a Star Schema Sales Data Mart in Snowflake — orchestrated end-to-end with Apache Airflow.


Key design principles:
- **Incremental Loading** — only new or changed records are processed on each pipeline run, minimizing compute cost and load time
- **SCD Type 2** — full history of changes is preserved on dimension table

---

## Architecture Overview

Data is extracted from two source systems — an **ERP** and a **CRM** — via Python ingestion scripts. Once landed in Snowflake's Bronze layer, **dbt** handles all transformations through to the Gold layer. Dimension tables in the Gold layer implement **SCD Type 2** to track full historical changes. Apache Airflow schedules and orchestrates the entire pipeline.
![ Architecture ](./imgs/architechture.png)




---

## Data Layers

The pipeline follows the **Medallion Architecture** with three layers inside Snowflake:

### 🥉 Bronze Layer — Raw Ingestion
Raw data landed as-is from source systems with no transformations. Tables are prefixed `brz_`.

### 🥈 Silver Layer — Cleaned & Conformed
Data is cleaned, typed, and standardized. Tables are prefixed `slv_`.

### 🥇 Gold Layer — Business-Ready Data Mart
Dimensional models ready for analytics and reporting.


--- 

## Data Lineage
The lineage flows from Bronze → Silver → Gold:

![ Data Lineage  ](./imgs/data_lineage.png)



---

## Data Mart Schema
 
The Gold layer implements a **Star Schema** Sales Data Mart:

![ Data Lineage  ](./imgs/Data%20Mart.drawio.png)


---
## Airflow DAG
 
The pipeline is orchestrated as a single Airflow DAG with the following task sequence:

![ Data Lineage  ](./imgs/airflow_dag.png)

| Task | Description |
|---|---|
| `erp_ingest_script` | Python script to extract and load ERP data into Bronze |
| `crm_ingest_script` | Python script to extract and load CRM data into Bronze |
| `dbt_bronze_erp` | dbt models to create Bronze ERP tables |
| `dbt_bronze_crm` | dbt models to create Bronze CRM tables |
| `dbt_silver_erp` | dbt transformations for ERP Silver layer |
| `dbt_silver_crm` | dbt transformations for CRM Silver layer |
| `dbt_silver_test` | dbt tests on Silver layer models |
| `dbt_gold_layer` | dbt models to build Gold dimensions and facts |
| `dbt_gold_test` | dbt tests on Gold layer models |



---

## Project Structure

```
.
├── config/
│   └── airflow.cfg                         # Airflow configuration
├── .env                                       # Local secrets — never commit
├── dags/
│   ├── connections.py                      # connect to Snowflake & Supabase connections
│   ├── dbt_dag.py                          # Main Airflow DAG
│   ├── ingest_crm.py                       # CRM ingestion script
│   ├── ingest_erp.py                       # ERP ingestion script
│   └── dbt/
│       └── pipe/                           # dbt project root
│           ├── dbt_project.yml
│           ├── sources.yml                 # Source definitions
│           ├── models/
│           │   ├── bronze/                 # Raw ingestion models 
│           │   │   ├── brz_crm_cust_info.sql
│           │   │   ├── brz_crm_prd_info.sql
│           │   │   ├── brz_crm_sales_details.sql
│           │   │   ├── brz_erp_cust_az12.sql
│           │   │   ├── brz_erp_loc_a101.sql
│           │   │   └── brz_erp_px_cat_g1v2.sql
│           │   ├── silver/                 # Cleaned & conformed models
│           │   │   ├── schema.yml
│           │   │   ├── slv_crm_cust_info.sql
│           │   │   ├── slv_crm_prd_info.sql
│           │   │   ├── slv_crm_sales_details.sql
│           │   │   ├── slv_erp_cust_az12.sql
│           │   │   ├── slv_erp_loc_a101.sql
│           │   │   └── slv_erp_px_cat_g1v2.sql
│           │   └── gold/                   # Business-ready Gold models
│           │       ├── dim_date.sql        # Date dimension
│           │       ├── dim_products.sql    # Product dimension (SCD2)
│           │       ├── fact_sales.sql      # Sales fact table 
│           │       └── ephemeral/
│           │           └── ephemeral_customers.sql  # customer CTE
│           ├── snapshots/
│           │   └── dim_customers.yml       # SCD2 snapshot for dim_customers
│           ├── macros/
│           │   ├── generate_schema_name.sql
│           │   └── tests/
│           │       ├── future_dates_check.sql
│           │       └── negative_values_check.sql
│           ├── tests/                      # Singular data tests
│           │   ├── assert_ord_dt_less_that_ship_due_dt.sql
│           │   ├── assert_start_dt_less_than_end_date_of_prd.sql
│           │   └── sales_price_check.sql
│           └── analyses/
│               └── adhock_queries.sql
├── dbt_profiles/
│   └── profiles.yml                        # dbt Snowflake connection profile
├── plugins/                                
├── docker-compose.yaml                     # Local environment setup
├── main.py
├── requirements.txt
├── pyproject.toml
└── uv.lock                                 
```

---

## Prerequisites

- Docker & Docker Compose
- Python 3.8+ with [uv](https://github.com/astral-sh/uv) (used for dependency management)
- Snowflake account with appropriate roles and warehouse configured


## Installation

```bash
# Clone the repository
git clone https://github.com/ahmedshawaly12/ETL_Pipeline-Using-dbt-Snowflake-Apache_Airflow.git
cd ETL_Pipeline-Using-dbt-Snowflake-Apache_Airflow

# Install dependencies with uv
uv sync

# Set up environment variables
touch .env
# Edit .env with your Snowflake and Supabase credentials

# Configure dbt Snowflake profile
cp dbt_profiles/profiles.yml ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml to match your Snowflake credentials

# Verify dbt connection
cd dags/dbt/pipe
dbt debug
```

### Running Locally with Docker

```bash
# Start Airflow and all services
docker-compose up -d

# Access Airflow UI
open http://localhost:8080
```

### Running the Pipeline

```bash
# Trigger the full pipeline via Airflow UI or CLI
airflow dags trigger dbt_dag

# Or run dbt manually layer by layer
cd dags/dbt/pipe
dbt run --select bronze
dbt run --select silver
dbt test --select silver
dbt snapshot                  
dbt run --select gold
dbt test --select gold
```

---



