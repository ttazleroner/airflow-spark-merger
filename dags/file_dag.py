from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
import os

def_args = {
    'owner': 'главный',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=25)
}

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
    
    archive_task  = BashOperator(
        task_id='archive_task',
        bash_command="""
        mkdir -p /home/jovyan/work/data/archive/{{ ds }} && \
        mv /home/jovyan/work/data/raw/*.csv /home/jovyan/work/data/archive/{{ ds }}/
    """
)
    
    def great_msg():
        print('данные готовы')
    
    end_task = PythonOperator(
        task_id='end_spark',
        python_callable=great_msg
    )
    
    start_task >> check_task >> clean_task >> join_task >> archive_task >> end_task

