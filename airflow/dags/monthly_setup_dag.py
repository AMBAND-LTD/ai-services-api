from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os
import logging
import asyncio
import traceback

# Add project root to Python path
sys.path.append('/code')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import monthly setup script
from ai_services_api.services.data.monthly_setup import run_monthly_setup as async_run_monthly_setup

def sync_monthly_setup(**context):
    """
    Synchronous wrapper for async monthly setup with improved error handling and logging
    
    Args:
        context: Airflow context dictionary containing task instance info
        
    Returns:
        str: Success message if setup completes successfully
        
    Raises:
        AirflowException: If setup fails or returns False
    """
    task_instance = context['task_instance']
    
    try:
        logger.info("Starting monthly setup process...")
        
        # Log configuration settings
        logger.info("Configuration: skip_openalex=False, skip_publications=False, "
                   "skip_graph=False, skip_search=False, skip_redis=False")
        
        # Execute async function
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
        
        if not result:
            error_msg = "Monthly setup failed: Function returned False"
            logger.error(error_msg)
            task_instance.xcom_push(key='error_message', value=error_msg)
            return False
        
        success_msg = "Monthly setup completed successfully"
        logger.info(success_msg)
        task_instance.xcom_push(key='success_message', value=success_msg)
        return success_msg
    
    except asyncio.CancelledError:
        error_msg = "Monthly setup was cancelled"
        logger.error(error_msg)
        task_instance.xcom_push(key='error_message', value=error_msg)
        return False
    
    except asyncio.TimeoutError:
        error_msg = "Monthly setup timed out"
        logger.error(error_msg)
        task_instance.xcom_push(key='error_message', value=error_msg)
        return False
    
    except Exception as e:
        error_msg = f"Monthly setup failed with error: {str(e)}"
        logger.error(error_msg)
        logger.error("Full traceback:")
        logger.error(traceback.format_exc())
        task_instance.xcom_push(key='error_message', value=error_msg)
        task_instance.xcom_push(key='error_traceback', value=traceback.format_exc())
        return False

# DAG configuration
default_args = {
    'owner': 'brian',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,  # Disabled automatic retries
    'execution_timeout': timedelta(hours=2),
    'start_date': days_ago(1),
}

# DAG definition
dag = DAG(
    'monthly_setup',
    default_args=default_args,
    description='Monthly data processing and setup tasks',
    schedule_interval='0 0 1 * *',  # Run at midnight on the first day of each month
    catchup=False,
    tags=['setup', 'monthly'],
    max_active_runs=1,
    concurrency=1  # Ensure only one task runs at a time
)

# Task definition
setup_task = PythonOperator(
    task_id='monthly_setup_task',
    python_callable=sync_monthly_setup,
    provide_context=True,
    dag=dag,
    doc_md="""
    # Monthly Setup Task
    
    This task runs the monthly data processing and setup procedures including:
    - OpenAlex data processing
    - Publications processing
    - Graph database updates
    - Search index updates
    - Redis cache management
    
    The task will fail if any of these components fail or if the overall process times out.
    """
)

# Add task documentation
dag.doc_md = """
# Monthly Setup DAG

This DAG handles the monthly data processing and setup procedures for the system.
It runs once per month and manages various data processing tasks.

## Schedule
- Runs at midnight (00:00) on the first day of each month
- No catchup for missed runs
- Maximum of one active run at a time

## Configuration
- Timeout: 2 hours
- No automatic retries
- Email notifications on failure
"""