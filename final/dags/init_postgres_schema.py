from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="init_postgres_schema",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["init", "schema"],
) as dag:

    init_schema = PostgresOperator(
        task_id="create_postgres_tables",
        postgres_conn_id="postgres_default",
        sql="sql/init.sql",
    )
