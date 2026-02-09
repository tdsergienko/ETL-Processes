from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

DATA_PATH = "/opt/airflow/data/IOT-temp.csv"
OUTPUT_PATH = "/opt/airflow/data/iot_temp_clean.csv"


def transform_cb(**context):
    df = pd.read_csv(DATA_PATH)

    df = df[df["out/in"] == "In"].reset_index(drop=True)

    date = pd.to_datetime(df["noted_date"], format="%d-%m-%Y %H:%M")
    df["date"] = date.dt.date

    p05 = df["temp"].quantile(0.05)
    p95 = df["temp"].quantile(0.95)
    df = df[(df["temp"] > p05) & (df["temp"] < p95)].reset_index(drop=True)

    df.to_csv(OUTPUT_PATH, index=False)

    print("\nFiltered data:")
    print(df.head())


with DAG(
    dag_id="task_04_transform_iot_temp",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["transform"],
) as dag:
    transform = PythonOperator(task_id="transform", python_callable=transform_cb)

    transform
