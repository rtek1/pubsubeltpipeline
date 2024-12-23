#2024-12-22 6:01pm
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, datetime
from google.cloud import pubsub_v1, bigquery
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from google.auth import default
from google.oauth2 import service_account
from google.cloud import storage

# Pub/Sub configurations
PROJECT_ID = 'pubsubeltpipeline'
SUBSCRIPTION_ID = 'pubsubamazonsales-sub'
ACK_DEADLINE = 60  # in seconds
POLL_INTERVAL = 5 * 60  # 5 minutes in seconds
DATASET_ID = 'amazonsales'
TABLE_ID = 'amazon_sales_data'

# Google Sheets configurations
SHEET_ID = '18PRupCHGYRaOvrvYfKEuBKXNrO7An-04WZsBWgHAAxM'
SHEET_NAME = 'amazon'

# Add after your existing configuration
DBT_PROJECT_DIR = '/home/airflow/gcs/dags/dbt/amazon_sales'  # This will be the path in Composer

def pull_pubsub_messages(**kwargs):
    from google.cloud import pubsub_v1

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    pulled_message = False  # Flag to track if a message was pulled

    def callback(message):
        nonlocal pulled_message
        print(f"Received message: {message.data.decode('utf-8')}")
        message.ack()
        pulled_message = True

    try:
        with subscriber:
            streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
            print(f"Listening for messages on {subscription_path}...")
            streaming_pull_future.result(timeout=ACK_DEADLINE)
    except TimeoutError:
        print("No messages available; streaming pull timed out.")
    except Exception as e:
        print(f"Unexpected exception during Pub/Sub message pull: {e}")
    finally:
        print("Finished polling Pub/Sub subscription.")

    # Push the result to XCom
    return pulled_message

def load_google_sheet_to_bigquery(**kwargs):
    # Check if a message was pulled
    ti = kwargs['ti']
    message_pulled = ti.xcom_pull(task_ids='pull_pubsub_messages')

    if not message_pulled:
        print("No message pulled. Skipping data load.")
        return

    # If a message was pulled, proceed with loading data
    from google.cloud import bigquery
    import pandas as pd
    import gspread
    from google.auth import default
    from google.oauth2 import service_account

    try:
        # Define the required scopes
        SCOPES = [
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/cloud-platform'
        ]
        
        # Get credentials with specific scopes
        credentials, project = default(scopes=SCOPES)
        
        # Initialize gspread client
        gc = gspread.authorize(credentials)
        
        # Open the Google Sheet.
        sheet = gc.open_by_key(SHEET_ID).worksheet(SHEET_NAME) 
        rows = sheet.get_all_values()
        headers = rows[0]
        data = rows[1:]

        # Convert to Pandas DataFrame
        df = pd.DataFrame(data, columns=headers)
        print("Extracted data from Google Sheets:")
        print(df.head())

        # Load into BigQuery
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()
        print(f"Loaded {len(df)} rows into BigQuery table {table_id}.")
    except Exception as e:
        print(f"Error loading data: {e}")
        raise

def run_dbt_transformations():
    """Run dbt transformations"""
    import subprocess
    import os
    from airflow.utils.log.logging_mixin import LoggingMixin
    
    logger = LoggingMixin().log
    
    logger.info(f"Starting dbt transformations in {DBT_PROJECT_DIR}")
    os.chdir(DBT_PROJECT_DIR)
    
    try:
        logger.info("Running dbt debug...")
        debug_result = subprocess.run(['dbt', 'debug'], capture_output=True, text=True)
        if debug_result.returncode != 0:
            raise Exception(f"dbt debug failed: {debug_result.stderr}")
        logger.info(debug_result.stdout)
        
        logger.info("Running dbt deps...")
        deps_result = subprocess.run(['dbt', 'deps'], capture_output=True, text=True)
        if deps_result.returncode != 0:
            raise Exception(f"dbt deps failed: {deps_result.stderr}")
        logger.info(deps_result.stdout)
        
        logger.info("Running dbt models...")
        result = subprocess.run(['dbt', 'run', '--full-refresh'], capture_output=True, text=True)
        logger.info(f"dbt run stdout: {result.stdout}")
        if result.stderr:
            logger.error(f"dbt run stderr: {result.stderr}")
        if result.returncode != 0:
            raise Exception(f"dbt run failed with return code {result.returncode}. \nStdout: {result.stdout}\nStderr: {result.stderr}")
        
    except Exception as e:
        logger.error(f"Error running dbt: {str(e)}")
        raise

# Default arguments
default_args = {
    'start_date': datetime(2024, 12, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pubsub_monitoring',
    default_args=default_args,
    description='Monitor Pub/Sub messages, extract Google Sheets data, and load into BigQuery.',
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    max_active_runs=2,
    catchup=False,
)

# Default health check task
default_health_check = BashOperator(
    task_id='health_check',
    bash_command='echo "Health Check Passed"',
    dag=dag,
)

# Pub/Sub pull task
pull_task = PythonOperator(
    task_id='pull_pubsub_messages',
    python_callable=pull_pubsub_messages,
    provide_context=True,
    dag=dag,
)

# Load data to BigQuery task
load_task = PythonOperator(
    task_id='load_google_sheet_to_bigquery',
    python_callable=load_google_sheet_to_bigquery,
    provide_context=True,
    dag=dag,
)

# Add new dbt task
dbt_transform_task = PythonOperator(
    task_id='run_dbt_transformations',
    python_callable=run_dbt_transformations,
    dag=dag,
)

# Define task dependencies
default_health_check >> pull_task >> load_task >> dbt_transform_task