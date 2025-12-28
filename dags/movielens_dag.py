"""
Airflow DAG for the MovieLens ELT Pipeline.
Task 7: Schedule daily runs of task 2 to task 6 using Airflow
Runs daily at 12:00 PM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define default arguments
default_args = {
    'owner': 'mmesoma',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 28),
    'email': ['mmesomakelvin@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'movielens_elt_pipeline',
    default_args=default_args,
    description='ELT pipeline for MovieLens data',
    schedule_interval='0 12 * * *',  # Run daily at 12:00 PM
    catchup=False,
    tags=['movielens', 'elt', 'pipeline'],
)

# Define the project path
PROJECT_PATH = '/home/mmesoma/movielens_elt_pipeline'
VENV_PYTHON = f'{PROJECT_PATH}/venv/bin/python'

# Task 1: Download data (not scheduled daily as data doesn't change)
# Uncomment if you want to re-download daily
# download_task = BashOperator(
#     task_id='download_data',
#     bash_command=f'{VENV_PYTHON} {PROJECT_PATH}/scripts/download_data.py',
#     dag=dag,
# )

# Task 2: Load data to staging tables
load_staging_task = BashOperator(
    task_id='load_staging',
    bash_command=f'{VENV_PYTHON} {PROJECT_PATH}/scripts/load_staging.py',
    dag=dag,
)

# Task 3: Transform/Clean data
transform_task = BashOperator(
    task_id='transform_data',
    bash_command=f'{VENV_PYTHON} {PROJECT_PATH}/scripts/transform_data.py',
    dag=dag,
)

# Task 4: Data quality checks
quality_task = BashOperator(
    task_id='data_quality',
    bash_command=f'{VENV_PYTHON} {PROJECT_PATH}/scripts/data_quality.py',
    dag=dag,
)

# Task 5: Create warehouse tables
warehouse_task = BashOperator(
    task_id='create_warehouse',
    bash_command=f'{VENV_PYTHON} {PROJECT_PATH}/scripts/create_warehouse.py',
    dag=dag,
)

# Task 6: Run analytics
analytics_task = BashOperator(
    task_id='run_analytics',
    bash_command=f'{VENV_PYTHON} {PROJECT_PATH}/scripts/run_analytics.py',
    dag=dag,
)

# Define task dependencies (order of execution)
# Task 2 -> Task 3 -> Task 4 -> Task 5 -> Task 6
load_staging_task >> transform_task >> quality_task >> warehouse_task >> analytics_task