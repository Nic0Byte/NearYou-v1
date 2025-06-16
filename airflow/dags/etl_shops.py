# src/etl_shops.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_data(**kwargs):
    overpass_query = """
    [out:json][timeout:25];
    area["name"="Milano"]->.searchArea;
    (
      node["shop"](area.searchArea);
      way["shop"](area.searchArea);
      relation["shop"](area.searchArea);
    );
    out center;
    """
    url = "http://overpass-api.de/api/interpreter"
    response = requests.post(url, data={'data': overpass_query})
    response.raise_for_status()
    data = response.json()
    return data.get("elements", [])

def transform_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='extract_data')
    transformed = []
    for element in raw_data:
        if element.get("type") == "node":
            lat = element.get("lat")
            lon = element.get("lon")
        elif "center" in element:
            lat = element["center"].get("lat")
            lon = element["center"].get("lon")
        else:
            continue
        tags = element.get("tags", {})
        transformed.append({
            "name": tags.get("name", "Non specificato"),
            "address": tags.get("addr:full", tags.get("addr:street", "Non specificato")),
            "category": tags.get("shop", "Non specificato"),
            "geom": f"POINT({lon} {lat})"
        })
    return transformed

def load_data(**kwargs):
    ti = kwargs['ti']
    shops = ti.xcom_pull(task_ids='transform_data')
    conn = psycopg2.connect(
        dbname="near_you_shops",
        user="nearuser",
        password="nearypass",
        host="postgres-postgis"
    )
    cur = conn.cursor()
    cur.execute("SET search_path TO public;")
    insert_query = """
      INSERT INTO shops (shop_name, address, category, geom)
      VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326))
      ON CONFLICT (shop_id) DO UPDATE 
         SET shop_name = EXCLUDED.shop_name,
             address = EXCLUDED.address,
             category = EXCLUDED.category,
             geom = EXCLUDED.geom;
    """
    for shop in shops:
        cur.execute(insert_query, (
            shop["name"],
            shop["address"],
            shop["category"],
            shop["geom"]
        ))
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    'etl_shops',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task
