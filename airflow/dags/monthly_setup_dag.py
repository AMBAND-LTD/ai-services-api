from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
import asyncio
import logging

# Add your project root to Python path
sys.path.append('/code')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import your monthly setup script
from ai_services_api.services.data.monthly_setup import run_monthly_setup

default_args = {
    'owner': 'brian',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'monthly_setup',
    default_args=default_args,
    description='Monthly data processing and setup tasks',
    schedule_interval='0 0 1 * *',  # Run at midnight on the first day of each month
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['setup', 'monthly'],
)

def execute_monthly_setup(**context):
    """Execute the monthly setup process"""
    try:
        asyncio.run(run_monthly_setup(
            expertise_csv='expertise.csv',
            skip_openalex=False,
            skip_publications=False,
            skip_graph=False,
            skip_search=False,
            skip_redis=False
        ))
        return "Monthly setup completed successfully"
    except Exception as e:
        logger.error(f"Monthly setup failed: {e}")
        raise

setup_task = PythonOperator(
    task_id='monthly_setup_task',
    python_callable=execute_monthly_setup,
    dag=dag,
)