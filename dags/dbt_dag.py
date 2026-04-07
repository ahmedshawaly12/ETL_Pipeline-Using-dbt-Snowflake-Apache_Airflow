from airflow.sdk import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime
import os


@dag(
    dag_id="dbt_dag",
    start_date= datetime(year=2026, month=4, day=1, tz="Africa/Cairo"),
    schedule="@daily",
    is_paused_upon_creation=False,
    catchup=True
)
def dbt_dag():

    DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dags/dbt/pipe")
    DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/.dbt")

    
    def dbt_run(model: str):
        return f"dbt run --select {model} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"

    def dbt_test(path: str):
        return f"dbt test --select {path} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"


    ## ingestion
    @task.bash
    def crm_ingest_script():
        return "echo hello"
    
    @task.bash
    def erp_ingest_script():
        return "echo hello"

    ## bronze 
    @task.bash
    def dbt_bronze_crm():    
        return dbt_run("brz_crm_cust_info brz_crm_prd_info brz_crm_sales_details")


    @task.bash
    def dbt_bronze_erp():    
        return dbt_run("brz_erp_cust_az12 brz_erp_loc_a101 brz_erp_px_cat_g1v2")

    ## silver 
    @task.bash
    def dbt_silver_crm():    
        return dbt_run("slv_crm_cust_info slv_crm_prd_info slv_crm_sales_details")


    @task.bash
    def dbt_silver_erp():    
        return dbt_run("slv_erp_cust_az12 slv_erp_loc_a101 slv_erp_px_cat_g1v2")


    ## gold
    @task.bash
    def dbt_gold_layer():         
        return """
        dbt run --select models/gold --project-dir /opt/airflow/dags/dbt/pipe --profiles-dir /opt/airflow/.dbt
        dbt snapshot --project-dir /opt/airflow/dags/dbt/pipe --profiles-dir /opt/airflow/.dbt
        """


    ## tests 
    @task.bash
    def dbt_silver_test():             
        return dbt_test("models/silver")

    @task.bash
    def dbt_gold_test():             
        return dbt_test("models/gold")

    ## creating dependencies
    ingestion = [crm_ingest_script(), erp_ingest_script()]
    bronze = [ dbt_bronze_crm(), dbt_bronze_erp() ]
    silver = [ dbt_silver_crm(), dbt_silver_erp() ]

    
   

    chain(ingestion, bronze, silver, dbt_silver_test(), dbt_gold_layer(), dbt_gold_test())


dbt_dag()