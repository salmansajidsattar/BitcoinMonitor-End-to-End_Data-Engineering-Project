from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from Scripts import data_etl

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),  # Start date of the DAG
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    dag_id='bitcoin_data_fetching',
    default_args=default_args,
    description='A DAG to run my_script.py every hour to fetch the data after every hour',
    schedule_interval=timedelta(hours=1),  # Run every hour
)

# Define the task
run_my_function = PythonOperator(
    task_id='run_my_function',
    python_callable=data_etl.run(),
    dag=dag,
)

# Set the task in the DAG
run_my_function
