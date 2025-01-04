from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os
import logging
import asyncio
import traceback

# Add your project root to Python path
sys.path.append('/code')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import your monthly setup script
from ai_services_api.services.data.monthly_setup import run_monthly_setup as async_run_monthly_setup

def sync_monthly_setup(**context):
    """Synchronous wrapper for async monthly setup"""
    try:
        # Use asyncio.run to execute the async function synchronously
        result = asyncio.run(
            async_run_monthly_setup(
                expertise_csv='/code/expertise.csv',
                skip_openalex=False,
                skip_publications=False,
                skip_graph=False,
                skip_search=False,
                skip_redis=False
            )
        )
        
        if result:
            logger.info("Monthly setup completed successfully")
            return "Monthly setup completed successfully"
        else:
            logger.error("Monthly setup returned False")
            raise Exception("Monthly setup failed")
    
    except Exception as e:
        logger.error(f"Monthly setup failed: {e}")
        logger.error(traceback.format_exc())
        raise

default_args = {
    'owner': 'brian',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    'monthly_setup',
    default_args=default_args,
    description='Monthly data processing and setup tasks',
    schedule_interval='0 0 1 * *',  # Run at midnight on the first day of each month
    catchup=False,
    tags=['setup', 'monthly'],
    max_active_runs=1,
)

setup_task = PythonOperator(
    task_id='monthly_setup_task',
    python_callable=sync_monthly_setup,
    provide_context=True,
    dag=dag,
)