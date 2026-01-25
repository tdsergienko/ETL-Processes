from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
from datetime import datetime
import requests
import xml.etree.ElementTree as ET

URL = "https://gist.githubusercontent.com/pamelafox/3000322/raw/6cc03bccf04ede0e16564926956675794efe5191/nutrition.xml"
PG_CONN_ID = "postgres_default"

PG_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS foods (
    name TEXT,
    manufacturer TEXT,
    serving_value NUMERIC,
    serving_units TEXT,
    calories_total NUMERIC,
    calories_fat NUMERIC,
    total_fat NUMERIC,
    saturated_fat NUMERIC,
    cholesterol NUMERIC,
    sodium NUMERIC,
    carbs NUMERIC,
    fiber NUMERIC,
    protein NUMERIC,
    vitamins JSONB,
    minerals JSONB
);
"""

PG_TRUNCATE = "TRUNCATE TABLE foods;"

PG_INSERT = """
INSERT INTO foods (
    name,
    manufacturer,
    serving_value,
    serving_units,
    calories_total,
    calories_fat,
    total_fat,
    saturated_fat,
    cholesterol,
    sodium,
    carbs,
    fiber,
    protein,
    vitamins,
    minerals
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s
);
"""

register_adapter(dict, Json)


def extract_cb(**context):
    res = requests.get(URL, timeout=10)
    res.raise_for_status()
    return res.text


def transform_cb(**context):
    xml_text = context["ti"].xcom_pull(task_ids="extract")
    root = ET.fromstring(xml_text)

    rows = []

    def text_as_float(el):
        if el is None or el.text is None:
            return None
        return float(el.text)

    def tag_as_float(el, tag):
        val = el.findtext(tag)
        return float(val) if val else None

    def attr_as_float(el, key):
        if el is None:
            return None
        val = el.attrib.get(key)
        return float(val) if val is not None else None

    def attr_as_str(el, key):
        return el.attrib.get(key) if el is not None else None

    def subelem_as_json(el, key):
        sub = el.find(key)
        if sub is None:
            return None
        return {child.tag: text_as_float(child) for child in sub}

    for food in root.findall("food"):
        name = food.findtext("name")
        mfr = food.findtext("mfr")

        serving_el = food.find("serving")
        serving_value = text_as_float(serving_el)
        serving_units = attr_as_str(serving_el, "units")

        calories_el = food.find("calories")
        calories_total = attr_as_float(calories_el, "total")
        calories_fat = attr_as_float(calories_el, "fat")

        total_fat = tag_as_float(food, "total-fat")
        saturated_fat = tag_as_float(food, "saturated-fat")
        cholesterol = tag_as_float(food, "cholesterol")
        sodium = tag_as_float(food, "sodium")
        carbs = tag_as_float(food, "carb")
        fiber = tag_as_float(food, "fiber")
        protein = tag_as_float(food, "protein")

        vitamins = subelem_as_json(food, "vitamins")
        minerals = subelem_as_json(food, "minerals")

        rows.append(
            (
                name,
                mfr,
                serving_value,
                serving_units,
                calories_total,
                calories_fat,
                total_fat,
                saturated_fat,
                cholesterol,
                sodium,
                carbs,
                fiber,
                protein,
                vitamins,
                minerals,
            )
        )

    return rows


def load_cb(**context):
    foods = context["ti"].xcom_pull(task_ids="transform")
    if not foods:
        return

    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
    with pg.get_conn() as con:
        with con.cursor() as cur:
            cur.execute(PG_CREATE_TABLE)
            cur.execute(PG_TRUNCATE)
            cur.executemany(PG_INSERT, foods)
        con.commit()


with DAG(
    dag_id="etl_flatten_foods_xml",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=extract_cb)
    transform = PythonOperator(task_id="transform", python_callable=transform_cb)
    load = PythonOperator(task_id="load", python_callable=load_cb)

    extract >> transform >> load
