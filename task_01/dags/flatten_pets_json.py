from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests

URL = (
    "https://raw.githubusercontent.com/LearnWebCode/json-example/master/pets-data.json"
)
PG_CONN_ID = "postgres_default"

PG_TRUNCATE = "TRUNCATE TABLE pets;"
PG_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS pets (
    name TEXT,
    species TEXT,
    birth_year INT,
    fav_food TEXT,
    photo TEXT
);
"""
PG_INSERT = """
INSERT INTO pets (name, species, birth_year, fav_food, photo)
VALUES (%s, %s, %s, %s, %s)
"""


def extract_cb(**context):
    res = requests.get(URL, timeout=10)
    res.raise_for_status()
    return res.json()


def transform_cb(**context):
    data = context["ti"].xcom_pull(task_ids="extract")
    pets = data.get("pets", [])

    res = []
    for pet in pets:
        foods = pet.get("favFoods") or [None]
        for food in foods:
            res.append(
                (
                    pet.get("name"),
                    pet.get("species"),
                    pet.get("birthYear"),
                    food,
                    pet.get("photo"),
                )
            )
    return res


def load_cb(**context):
    pets = context["ti"].xcom_pull(task_ids="transform")
    if not pets:
        return

    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with pg.get_conn() as con:
        with con.cursor() as cur:
            cur.execute(PG_CREATE_TABLE)
            cur.execute(PG_TRUNCATE)
            cur.executemany(PG_INSERT, pets)
        con.commit()


with DAG(
    dag_id="etl_flatten_pets_json",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=extract_cb)
    transform = PythonOperator(task_id="transform", python_callable=transform_cb)
    load = PythonOperator(task_id="load", python_callable=load_cb)

    extract >> transform >> load
