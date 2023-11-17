from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def my_python_function():
    print("Hello World")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('my_airflow_dag', default_args=default_args, schedule_interval=None) as dag:
    start_task = DummyOperator(task_id='start_task')
    python_task = PythonOperator(task_id='python_task', python_callable=my_python_function)
    end_task = DummyOperator(task_id='end_task')

    start_task >> python_task >> end_task

