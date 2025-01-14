from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import asyncio

# Import the monthly setup function
from ai_services_api.services.centralized_repository.monthly_setup import run_monthly_setup

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def execute_monthly_setup(**kwargs):
    """
    Wrapper function to execute monthly setup in Airflow context
    
    This function runs the async monthly setup in a way compatible with 
    Airflow's PythonOperator
    """
    try:
        # Run the async function using asyncio
        result = asyncio.run(run_monthly_setup(
            skip_openalex=False,
            skip_publications=False,
            skip_graph=False,
            skip_search=False,
            skip_redis=False,
            skip_topics=False
        ))
        
        # You can add any additional logging or context passing here
        print("Monthly setup completed successfully")
        return result
    
    except Exception as e:
        # Log the error and re-raise to ensure Airflow marks the task as failed
        print(f"Error in monthly setup: {e}")
        raise

# Create the DAG
with DAG(
    'monthly_research_processing',
    default_args=default_args,
    description='Monthly research data processing pipeline',
    schedule_interval='0 0 1 * *',  # Run at midnight on the first day of each month
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['research', 'monthly'],
    params={},  # Explicitly add params to avoid NoneType error
) as dag:
    
    # Define the task
    process_task = PythonOperator(
        task_id='monthly_setup',
        python_callable=execute_monthly_setup,
        provide_context=True,  # Allow access to Airflow context
    )

    # The task is the DAG's main (and only) task
    process_task