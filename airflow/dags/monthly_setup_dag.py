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
    Synchronous wrapper for async monthly setup with comprehensive error handling
    """
    task_instance = context['task_instance']
    
    try:
        logger.info("Starting monthly setup process...")
        
        # Execute async function with comprehensive settings
        result = asyncio.run(
            async_run_monthly_setup(
                expertise_csv='/code/expertise.csv',
                skip_openalex=False,
                skip_publications=False,
                skip_graph=False,
                skip_search=False,
                skip_redis=False,
                skip_topics=False
            )
        )
        
        if not result:
            error_msg = "Monthly setup failed: Function returned False"
            logger.error(error_msg)
            task_instance.xcom_push(key='error_message', value=error_msg)
            raise Exception(error_msg)
        
        success_msg = "Monthly setup completed successfully"
        logger.info(success_msg)
        task_instance.xcom_push(key='success_message', value=success_msg)
        return success_msg
    
    except Exception as e:
        error_msg = f"Monthly setup failed with error: {str(e)}"
        logger.error(error_msg)
        logger.error("Full traceback:")
        logger.error(traceback.format_exc())
        task_instance.xcom_push(key='error_message', value=error_msg)
        task_instance.xcom_push(key='error_traceback', value=traceback.format_exc())
        raise

# DAG configuration
default_args = {
    'owner': 'research_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'execution_timeout': timedelta(hours=2),
    'start_date': days_ago(1),
}

# DAG definition
dag = DAG(
    'monthly_research_setup',
    default_args=default_args,
    description='Monthly Research Data Processing and Setup',
    schedule_interval='0 0 1 * *',  # Run at midnight on the first day of each month
    catchup=False,
    tags=['research', 'data_processing', 'monthly'],
    max_active_runs=1,
    concurrency=1
)

# Task definition
setup_task = PythonOperator(
    task_id='monthly_research_setup_task',
    python_callable=sync_monthly_setup,
    provide_context=True,
    dag=dag,
    doc_md="""
    # Monthly Research Setup Task
    
    Comprehensive monthly data processing including:
    - Expert data processing
    - Publications retrieval
    - Topic classification
    - Graph database updates
    - Search index creation
    - Redis cache management
    """
)

# DAG documentation
dag.doc_md = """
# Monthly Research Data Processing DAG

Handles comprehensive monthly research data setup and processing.

## Key Components
- Expert data enrichment
- Publications retrieval from multiple sources
- Topic classification
- Graph database updates
- Search index creation
- Redis cache management

## Schedule
- Runs at midnight on the first day of each month
- No catchup for missed runs
- Maximum of one active run at a time

## Configuration
- 2-hour timeout
- No automatic retries
- Email notifications on failure
"""