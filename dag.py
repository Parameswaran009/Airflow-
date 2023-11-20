from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import boto3
import os

def fetch_code_from_s3(bucket_name, object_key, local_path):
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, object_key, local_path)

def my_python_function(**kwargs):
    # Replace 'your-s3-bucket' and 'path/to/your/code.py' with your S3 bucket and path
    fetch_code_from_s3('mwaa-environmentbucket-beynhcbcqflf', 's3://mwaa-environmentbucket-beynhcbcqflf/dag.py', '/home/ubuntu/airflow/code/code.py')
    
    # Replace '/var/lib/airflow/code/' with your desired local path
    local_path = '/home/ubuntu/airflow/code/'
    
    # Execute the downloaded Python code
    exec(open(os.path.join(local_path, 'code.py')).read(), globals(), locals())
    print("Python code executed successfully")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('my_airflow_dag', default_args=default_args, schedule_interval=None) as dag:
    start_task = DummyOperator(task_id='start_task')
    python_task = PythonOperator(task_id='python_task', python_callable=my_python_function, provide_context=True)
    end_task = DummyOperator(task_id='end_task')

    start_task >> python_task >> end_task


