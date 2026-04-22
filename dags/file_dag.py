from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
import os
import boto3
import dotenv
from botocore.client import Config

silver_path = "/home/jovyan/work/data/silver/transactions_cleaned.parquet"

def_args = {
    'owner': 'главный',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=25)
}

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
        
def check_file():
    raw_path = '/home/jovyan/work/data/raw/'
    if not os.path.exists(raw_path):
        return False
    file = [f for f in os.listdir(raw_path) if f.endswith('.csv')]
    return len(file)>0

with DAG(
    'spark_pipeline',
    default_args=def_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    start_task = BashOperator(
        task_id='start_spark',
        bash_command= 'echo "додониднид"'
    )
    clean_task = BashOperator(
        task_id='clean_spark',
        bash_command='docker exec spark_single spark-submit /home/jovyan/work/dags/cl_data.py'
    )
    
    join_task = BashOperator(
        task_id='join_table',
        bash_command='docker exec spark_single spark-submit /home/jovyan/work/user/user_data.py'
    )
    
    check_task = ShortCircuitOperator(
        task_id='check_spark',
        python_callable=check_file
    )
    
    archive_task  = PythonOperator(
        task_id='archive_task',
        python_callable=load_minio
)
    
    def great_msg():
        print('данные готовы')
    
    end_task = PythonOperator(
        task_id='end_spark',
        python_callable=great_msg
    )
    
    start_task >> check_task >> clean_task >> join_task >> archive_task >> end_task

