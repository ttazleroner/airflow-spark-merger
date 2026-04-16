from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime

@dag(
    start_date=datetime(2026, 1, 1),
    dag_id='bimbimbim',
    schedule=None,
    catchup=False,
    tags=['spark', 'bimbim']
)

def spark_pipeline():
    
    run_spark_job = BashOperator(
        task_id='spark_task',
        bash_command="docker exec spark_single spark-submit /home/jovyan/work/dags/cl_data.py"
    )
    
    @task
    def notify_suc():
        print('я мячик')
    
    run_spark_job >> notify_suc()

spark_pipeline()