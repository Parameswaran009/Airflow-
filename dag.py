from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import boto3
import os

def fetch_code_from_s3(bucket_name, object_key, local_path):
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, object_key, local_path)

def execute_python_code(local_path):
    # Execute the downloaded Python code
    exec(open(local_path).read(), globals(), locals())
    print("Python code executed successfully")

def my_python_function(**kwargs):
    # Replace 'your-s3-bucket' and 'path/to/your/code.py' with your S3 bucket and path
    s3_bucket = 'mwaa-environmentbucket-beynhcbcqflf'
    s3_key = 's3://mwaa-environmentbucket-beynhcbcqflf/dag.py'
    local_path = '/home/ubuntu/airflow/code/dag.py'

    # Fetch code from S3
    fetch_code_from_s3(s3_bucket, s3_key, local_path)

    # Execute the downloaded Python code
    execute_python_code(local_path)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,  # Set retries to 0 to disable retries
}

with DAG('my_airflow_dag', default_args=default_args, schedule_interval=None) as dag:
    start_task = DummyOperator(task_id='start_task')
    python_task = PythonOperator(task_id='python_task', python_callable=my_python_function, provide_context=True, retries=0)  # Set retries to 0
    end_task = DummyOperator(task_id='end_task')

    start_task >> python_task >> end_task

