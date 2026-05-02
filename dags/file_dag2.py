from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import boto3
import dotenv
from botocore.client import Config

def load_minio():
    endpoint = 'http://minio:9000'
    minio_key = os.getenv('MINIO_USER')
    minio_secretkey = os.getenv('MINIO_PASSWORD')
    bucket_name = 'raw-archive'
    raw_path = '/home/jovyan/work/data/raw/'
    
    s3 = boto3.resource('s3',
        endpoint_url=endpoint,
        aws_access_key_id=minio_key,
        aws_secret_access_key=minio_secretkey,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    if s3.Bucket(bucket_name) not in s3.buckets.all():
        s3.create_bucket(Bucket=bucket_name)

    files = [f for f in os.listdir(raw_path) if f.endswith('.csv')]

    for file_name in files:
        f_path = os.path.join(raw_path, file_name)
        object_key = f"{datetime.now().strftime('%Y-%m-%d')}/{file_name}"
        print(f"загрузка {file_name} в бакет {bucket_name}")
        s3.Bucket(bucket_name).upload_file(f_path, object_key)
        os.remove(f_path)
        print(f'файл {file_name} архивирован')


with DAG(
    'spark_streaming_scheduler', 
    schedule_interval='*/10 * * * *', 
    start_date=datetime(2026, 1, 1), 
    catchup=False
) as dag_streaming:

    streaming_kafka = BashOperator(
        task_id='streaming_kafka',
        bash_command='docker exec spark_single spark-submit /home/jovyan/work/dags/streaming_kafka.py'
    )

    archive_task = PythonOperator(
        task_id='archive_task',
        python_callable=load_minio
    )

    streaming_kafka >> archive_task