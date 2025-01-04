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
from ai_services_api.services.data.monthly_setup import run_monthly_setup

default_args = {
    'owner': 'brian',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
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

def execute_monthly_setup(**context):
    """Execute the monthly setup process synchronously with enhanced logging"""
    logger.info("Starting monthly setup process")
    
    # Prepare environment variables logging
    env_vars = [
        'DATABASE_URL', 'NEO4J_URI', 'NEO4J_USER', 
        'REDIS_URL', 'OPENALEX_API_URL', 
        'GEMINI_API_KEY', 'ORCID_CLIENT_ID'
    ]
    
    logger.info("Environment Variables Check:")
    for var in env_vars:
        value = os.getenv(var, 'NOT SET')
        logger.info(f"{var}: {'*' * len(value) if value != 'NOT SET' else value}")
    
    # Create a new event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # Timeout the async operation after 30 minutes
        result = loop.run_until_complete(
            asyncio.wait_for(
                run_monthly_setup(
                    expertise_csv='/code/expertise.csv',
                    skip_openalex=False,
                    skip_publications=False,
                    skip_graph=False,
                    skip_search=False,
                    skip_redis=False
                ),
                timeout=1800  # 30 minutes
            )
        )
        
        if result:
            logger.info("Monthly setup completed successfully")
            return "Monthly setup completed successfully"
        else:
            logger.error("Monthly setup returned False")
            raise Exception("Monthly setup failed")
    
    except asyncio.TimeoutError:
        logger.error("Monthly setup timed out after 30 minutes")
        raise Exception("Monthly setup timed out")
    
    except Exception as e:
        logger.error(f"Monthly setup failed: {e}")
        logger.error(traceback.format_exc())
        raise
    
    finally:
        loop.close()

setup_task = PythonOperator(
    task_id='monthly_setup_task',
    python_callable=execute_monthly_setup,
    provide_context=True,
    dag=dag,
)