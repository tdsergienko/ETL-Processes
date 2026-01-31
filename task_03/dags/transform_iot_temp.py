from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

DATA_PATH = "/opt/airflow/data/IOT-temp.csv"


def transform_cb(**context):
    df = pd.read_csv(DATA_PATH)
    print("Raw data:")
    print(df.head())

    out = df[df["out/in"] == "Out"]
    tmp = (
        out[["noted_date", "temp"]]
        .groupby(by="noted_date")
        .mean()
        .reset_index()
        .sort_values(by="temp")
        .reset_index(drop=True)
    )
    print("\n5 coldest days:")
    print(tmp.head(5))
    print("\n5 hottest days:")
    print(tmp.tail(5))

    df = df[df["out/in"] == "In"].reset_index(drop=True)
    print("\nIN data:")
    print(df.head())

    date = pd.to_datetime(df["noted_date"], format="%d-%m-%Y %H:%M")
    df["date"] = date.dt.date
    print("\nParsed dates:")
    print(df[["noted_date", "date"]].head())

    p05 = df["temp"].quantile(0.05)
    p95 = df["temp"].quantile(0.95)
    print(f"\nTemperature 5th percentile: {p05}")
    print(f"Temperature 95th percentile: {p95}")

    df = df[(df["temp"] > p05) & (df["temp"] < p95)].reset_index(drop=True)
    print("\nFiltered data:")
    print(df.head())


with DAG(
    dag_id="task_03_transform_iot_temp",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    transform = PythonOperator(task_id="transform", python_callable=transform_cb)

    transform
