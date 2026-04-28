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
import shutil

silver_path = "/home/jovyan/work/data/silver/transactions_cleaned.parquet"

def_args = {
    'owner': 'главный',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=25)
}

def csv_recovery():
    raw_path = '/home/jovyan/work/data/raw/dirty_transactions_1gb.csv'
    recovery_path = '/home/jovyan/work/data/master/dirty_transactions_1gb.csv'
    os.makedirs(os.path.dirname(raw_path), exist_ok=True)
    if os.path.exists(raw_path):
        try:
            os.remove(raw_path)
        except:
            pass 
    shutil.copyfile(recovery_path, raw_path)


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

def kafka_othet_to_minio():
    endpoint = 'http://minio:9000'
    minio_key = os.getenv('MINIO_USER')
    minio_secretkey = os.getenv('MINIO_PASSWORD')
    bucket2_name = 'silver-transactions'
    output_path = '/home/jovyan/work/data/silver/transactions'

    s3 = boto3.resource('s3',
    endpoint_url=endpoint,
    aws_access_key_id=minio_key,
    aws_secret_access_key=minio_secretkey,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
    )
    try:
        s3.meta.client.head_bucket(Bucket=bucket2_name)
    except:
        s3.create_bucket(Bucket=bucket2_name)

    if os.path.exists(output_path):
        files = [f for f in os.listdir(output_path) if f.endswith('.parquet')]
        print(f"найдено {len(files)} файлов для загрузки в MinIO")
        for file_name in files:
            f_path = os.path.join(output_path, file_name)
            object_key = f"{datetime.now().strftime('%Y-%m-%d')}/{file_name}"
            s3.Bucket(bucket2_name).upload_file(f_path, object_key)
            os.remove(f_path)
    else:
        print('папка уже существует')

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

    spark_kafka_to_silver = BashOperator(
        task_id='spark_kafka_to_silver',
        bash_command='docker exec spark_single spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /home/jovyan/work/dags/kafka_spark.py'
    )

    producer_task = BashOperator(
        task_id='run_producer_to_kafka',
        bash_command='docker exec spark_single python /home/jovyan/work/kafka_producer.py'
    )

    archive_kafka = PythonOperator(
        task_id='archive_kafka',
        python_callable=kafka_othet_to_minio
    )

    clean_task = BashOperator(
        task_id='clean_spark',
        bash_command='docker exec spark_single spark-submit --driver-memory 2g --executor-memory 2g /home/jovyan/work/dags/cl_data.py'
    )

    recovery_csv = PythonOperator(
        task_id='recovery_csv',
        python_callable=csv_recovery
    )
    
    join_task = BashOperator(
        task_id='join_table',
        bash_command='docker exec spark_single spark-submit /home/jovyan/work/user/user_data.py'
    )

    join_analytics = BashOperator(
        task_id='join_analytics',
        bash_command='docker exec spark_single spark-submit /home/jovyan/work/user/join_ebat.py'
    )

    clean_checkpoint = BashOperator(
        task_id='clean_checkpoints',
        bash_command='docker exec spark_single rm -rf /home/jovyan/work/data/checkpoints/raw_to_silver'
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

    install_libs = BashOperator(
        task_id='install_libs',
        bash_command='docker exec spark_single pip install kafka-python-ng'
    )
    start_task >> install_libs >> recovery_csv >> clean_checkpoint >> check_task >> producer_task >> spark_kafka_to_silver >> archive_kafka >> clean_task >> join_task >> join_analytics >> archive_task >> end_task