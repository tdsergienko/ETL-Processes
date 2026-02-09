from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

DATA_PATH = "/opt/airflow/data/iot_temp_clean.csv"


def load_cb():
    df = pd.read_csv(DATA_PATH, parse_dates=["date"])

    print("Load all historical data")
    print(f"Total rows: {len(df)}")

    daily = (
        df.groupby("date", as_index=False)
        .agg(avg_temp=("temp", "mean"))
        .sort_values("avg_temp")
    )

    print("\n5 coldest days:")
    print(daily.head(5).to_string(index=False))

    print("\n5 hottest days:")
    print(daily.tail(5).to_string(index=False))


with DAG(
    dag_id="task_04_load_full",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["load", "full"],
) as dag:
    load_full = PythonOperator(
        task_id="load_full",
        python_callable=load_cb,
    )

    load_full
