from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

CLEAN_PATH = "/opt/airflow/data/iot_temp_clean.csv"
FAKE_TODAY = "2018-09-11"
DAYS_BACK = 3


def load_cb():
    df = pd.read_csv(CLEAN_PATH, parse_dates=["date"])

    today = pd.to_datetime(FAKE_TODAY)
    cutoff = today - pd.Timedelta(days=DAYS_BACK)

    df_inc = df[(df["date"] >= cutoff) & (df["date"] <= today)]

    print("INCREMENTAL LOAD")
    print(f"Fake today: {today.date()}")
    print(f"Loading data from: {cutoff.date()}")
    print(f"Rows: {len(df_inc)}")

    daily = (
        df_inc.groupby("date", as_index=False)
        .agg(avg_temp=("temp", "mean"))
        .sort_values("avg_temp")
    )

    print(f"\nLast {DAYS_BACK} days:")
    print(daily.to_string(index=False))


with DAG(
    dag_id="task_04_load_last",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["load", "incremental"],
) as dag:
    load_incremental = PythonOperator(
        task_id="load_incremental",
        python_callable=load_cb,
    )

    load_incremental
