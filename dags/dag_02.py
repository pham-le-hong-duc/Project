from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
# sys.path.append('/opt/airflow/plugins')
# sys.path.append('/mnt/d/learn/DE/Semina_project/ELT/extract')
# sys.path.append('/mnt/d/learn/DE/Semina_project/ELT/load')
from ELT.extract.script.crawl_stream import run_kafka_collector
from ELT.load.load_to_minio import load_data_to_minio
RUNTIME_SECONDS = 3300  # 55 phút
default_args = {
    'owner': 'trading-data',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    '01_ingest_kafka_minio',
    default_args=default_args,
    schedule_interval='0 * * * *', # Chạy đầu mỗi giờ
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1
) as dag:

    task_crawl = PythonOperator(
        task_id='crawl_websocket_to_kafka',
        python_callable=run_kafka_collector
    )

    task_load = PythonOperator(
        task_id='load_to_minio',
        python_callable=load_data_to_minio,
        op_kwargs={
            'kafka_bootstrap': 'redpanda:29092',
            's3_endpoint': 'http://minio:9000',
            'runtime_seconds': RUNTIME_SECONDS
        }
    )
    #parallel
    [task_crawl, task_load]