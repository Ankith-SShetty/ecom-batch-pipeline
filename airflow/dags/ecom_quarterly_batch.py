from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import psycopg2
import logging

logger = logging.getLogger(__name__)

PG_CONN = {
    'host':     'postgres',
    'port':     5432,
    'user':     'ecom_user',
    'password': 'ecom_pass',
    'dbname':   'ecom_db'
}

default_args = {
    'owner':            'ecom_pipeline',
    'depends_on_past':  False,
    'start_date':       days_ago(1),
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': False,
}

dag = DAG(
    dag_id='ecom_quarterly_batch',
    default_args=default_args,
    description='Quarterly e-commerce batch processing pipeline',
    schedule_interval='@quarterly',
    catchup=False,
    tags=['ecom', 'batch', 'ml-pipeline'],
)

def validate_raw_data(**kwargs):
    conn   = psycopg2.connect(**PG_CONN)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM raw_transactions;")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    logger.info(f'Raw transaction count: {count}')
    if count < 1000:
        raise ValueError(f'Not enough raw data to process. Found only {count} records.')
    logger.info('Validation passed. Proceeding to Spark job.')
    return count

run_spark = BashOperator(
    task_id='run_spark_batch_job',
    bash_command=(
        'docker exec spark '
        'spark-submit '
        '--master local[*] '
        '--packages org.postgresql:postgresql:42.6.0 '
        '/opt/spark-jobs/batch_job.py'
    ),
    dag=dag,
)

def verify_output(**kwargs):
    conn   = psycopg2.connect(**PG_CONN)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM feature_set_quarterly;")
    count = cursor.fetchone()[0]
    cursor.execute("SELECT MAX(processed_at) FROM feature_set_quarterly;")
    last_run = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    logger.info(f'Feature rows: {count} | Last processed: {last_run}')
    if count == 0:
        raise ValueError('Spark job produced no output rows in feature_set_quarterly.')
    logger.info('Output verification passed.')
    return count

def log_success(**kwargs):
    logger.info('Pipeline completed successfully. Feature set is ready for the ML model.')

t1_validate = PythonOperator(
    task_id='validate_raw_data',
    python_callable=validate_raw_data,
    dag=dag,
)

t3_verify = PythonOperator(
    task_id='verify_output',
    python_callable=verify_output,
    dag=dag,
)

t4_success = PythonOperator(
    task_id='log_success',
    python_callable=log_success,
    dag=dag,
)

t1_validate >> run_spark >> t3_verify >> t4_success
