from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': 0,
}

# Instantiate the DAG
dag = DAG(
    'my_dag',
    default_args=default_args,
    catchup=False,  # Prevent backfilling
)

# Define tasks
start_task = DummyOperator(task_id='start_task', dag=dag)

def my_python_function(**kwargs):
    # Your Python code goes here
    print("Hello from my Python function!")

python_task = PythonOperator(
    task_id='python_task',
    python_callable=my_python_function,
    provide_context=True,
    dag=dag,
)

end_task = DummyOperator(task_id='end_task', dag=dag)

# Define the task dependencies
start_task >> python_task >> end_task

