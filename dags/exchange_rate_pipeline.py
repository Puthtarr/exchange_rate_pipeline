from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# ไม่ต้องเพิ่ม sys.path เอง เพราะเราตั้ง PYTHONPATH ไว้ใน docker แล้ว
from src.ingest.fetch_exchange_rate import fetch_exchage_rate
from src.upload.upload_to_minio import upload_to_minio
from src.upload.upload_to_postgres import upload_to_postgres
from src.transform.clean_exchange_rate import *

def fetch():
    fetch_exchage_rate()

with DAG(
    dag_id='exchange_rate_pipeline',
    start_date=datetime(2025, 6, 14),
    schedule_interval=None,
    catchup=False,
    tags=['exchange-rate']
) as dag:
    t1 = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch
    )
