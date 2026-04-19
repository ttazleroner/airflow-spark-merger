from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def_args = {
    'owner': 'главный',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=25)
}

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
    
    def great_msg():
        print('данные готовы')
    
    end_task = PythonOperator(
        task_id='end_spark',
        python_callable=great_msg
    )
    
    start_task >> clean_task >> end_task

