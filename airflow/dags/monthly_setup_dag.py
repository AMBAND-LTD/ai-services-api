from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import asyncio

# Import the existing setup script
from monthly_setup import run_monthly_setup

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def execute_monthly_setup():
    """Execute the monthly setup process"""
    asyncio.run(run_monthly_setup(
        skip_openalex=False,
        skip_publications=False,
        skip_graph=False,
        skip_search=False,
        skip_redis=False,
        skip_topics=False
    ))

with DAG(
    'monthly_research_processing',
    default_args=default_args,
    description='Monthly research data processing pipeline',
    schedule_interval='0 0 1 * *',  # Run at midnight on the first day of each month
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['research', 'monthly'],
) as dag:
    
    process_task = PythonOperator(
        task_id='monthly_setup',
        python_callable=execute_monthly_setup,
    )
    
    # Single task that runs everything
    process_task