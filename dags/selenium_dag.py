from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from datetime import datetime, timedelta
import subprocess
import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import asyncio

def run_ozon():
    try:
        subprocess.run(["python3", "/home/uidserg/airflow/scripts/ozon.py"])
    except Exception as e:
        print(f"Ошибка при выполнении ozon.py: {e}")

def run_wb():
    try:
        subprocess.run(["python3", "/home/uidserg/airflow/scripts/wb.py"])
    except Exception as e:
        print(f"Ошибка при выполнении wb.py: {e}")
        
def search_data():
    try:
        subprocess.run(["python3", "/home/uidserg/airflow/scripts/search_data1.py"])
    except Exception as e:
        print(f"Ошибка при выполнении search_data1.py: {e}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'retries': 1,
}

dag = DAG(
    'parse_price',
    default_args=default_args,
    schedule_interval='0 10 * * 1',  # Каждый понедельник в 10:00
)

run_script_ozon = PythonOperator(
    task_id='run_ozon',
    python_callable=run_ozon,
    dag=dag,
)

run_script_wb = PythonOperator(
    task_id='run_wb',
    python_callable=run_wb,
    dag=dag,
)

search_data = PythonOperator(
    task_id='search_data',
    python_callable=search_data,
    dag=dag,
)

run_script_ozon >> run_script_wb >> search_data