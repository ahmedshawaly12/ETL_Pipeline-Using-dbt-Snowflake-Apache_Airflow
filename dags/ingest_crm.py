import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from connections import get_snowflake_connection, get_postgres_connection


CRM_TABLES = (
    'crm_cust_info',
    'crm_prd_info',
    'crm_sales_details'
)


def ingest_crm():
    sf_conn = get_snowflake_connection()
    pg_conn = get_postgres_connection()

    cursor = sf_conn.cursor()

    try:
        for table_name in CRM_TABLES:
            print(f"Processing CRM table: {table_name}")

            df = pd.read_sql(f"SELECT * FROM {table_name}", pg_conn)

            cols = ", ".join(
                [f'"{col.upper()}" STRING' for col in df.columns]
            )

            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name.upper()} ({cols})
            """)

            write_pandas(sf_conn, df, table_name.upper())

            print(f"{table_name} transferred successfully!")

    finally:
        cursor.close()
        sf_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    ingest_crm()