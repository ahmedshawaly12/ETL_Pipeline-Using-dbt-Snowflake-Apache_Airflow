import snowflake.connector
import psycopg2
import warnings

warnings.filterwarnings("ignore")


def get_snowflake_connection():
    return snowflake.connector.connect(
        user='dbt_user',
        password='PassWord!',
        account='QOOKLEG-XH49346',
        warehouse='dbt_wh',
        database='DataWarehouse',
        schema='staging'
    )


def get_postgres_connection():
    return psycopg2.connect(
        host="aws-1-us-east-1.pooler.supabase.com",
        database="postgres",
        user="postgres.xjeudtqlwcctjrwfgfpj",
        password="FgZY4@c+QjMzQkr",
        port=6543
    )