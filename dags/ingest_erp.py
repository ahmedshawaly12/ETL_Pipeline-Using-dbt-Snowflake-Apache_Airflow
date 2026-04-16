import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from connections import get_snowflake_connection, get_postgres_connection


ERP_TABLES = (
    'erp_cust_az12',
    'erp_loc_a101',
    'erp_px_cat_g1v2'
)


def ingest_erp():
    sf_conn = get_snowflake_connection()
    pg_conn = get_postgres_connection()

    cursor = sf_conn.cursor()

    try:
        for table_name in ERP_TABLES:
            print(f"Processing ERP table: {table_name}")

            # Extract
            df = pd.read_sql(f"SELECT * FROM {table_name}", pg_conn)

            # Create table dynamically
            cols = ", ".join(
                [f'"{col.upper()}" STRING' for col in df.columns]
            )

            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name.upper()} ({cols})
            """)

            # Load
            write_pandas(sf_conn, df, table_name.upper())

            print(f"{table_name} transferred successfully!")

    finally:
        cursor.close()
        sf_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    ingest_erp()
    