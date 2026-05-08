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

def_args = {
    'owner': 'главный',
    'start_date': datetime(2026,1,1)
}

def end_msg():
        print('просмотр таблицы готов')

check_cmnd = (
    "docker exec -e ICEBERG_DB_PASS='airflow' "
    "-e AWS_ACCESS_KEY_ID=\"$MINIO_USER\" -e AWS_SECRET_ACCESS_KEY=\"$MINIO_PASSWORD\" "
    "spark_single spark-submit "
    "--packages "
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.postgresql:postgresql:42.6.0 "
    "/home/jovyan/work/dags/checkSQL_table.py"
)

with DAG(
    'check_table',
    default_args=def_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    
    checking_table = BashOperator(
        task_id='check_table',
        bash_command=check_cmnd
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=end_msg
    )

    checking_table >> end_task