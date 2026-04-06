from airflow.sdk import dag, task


@dag(dag_id="dbt_dag")
def dbt_dag():

    @task.python
    def run_dbt():
        from dbt.cli.main import dbtRunner, dbtRunnerResult

        runner = dbtRunner()
        result: dbtRunnerResult = runner.invoke(
            ["run",
             "--project-dir", "/opt/airflow/dags/dbt/pipe",
             "--profiles-dir", "/opt/airflow/.dbt"]
        )

        if not result.success:
            raise Exception(f"dbt run failed: {result.exception}")

        print("dbt run completed successfully")

    run_dbt()


dbt_dag()