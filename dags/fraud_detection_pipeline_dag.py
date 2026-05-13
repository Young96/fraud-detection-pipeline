
"""
Fraud Detection Pipeline - Airflow DAG

Orchestrates the daily fraud detection pipeline using Medallion architecture.
Schedule: Daily at 2 AM
Owner: Data Engineering Team
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# Default arguments applied to all tasks
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['de-team@company.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}


# DAG configuration
dag = DAG(
    dag_id='fraud_detection_pipeline',
    description='End-to-end fraud detection pipeline with medallion architecture',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['fraud_detection', 'production', 'daily', 'finance'],
)


def ingest_bronze_layer(**context):
    """Ingest raw transaction data into Bronze layer."""
    print("Starting Bronze Layer Ingestion")
    # In production: spark-submit bronze_ingestion.py
    context['task_instance'].xcom_push(
        key='bronze_row_count', value=284807
    )
    print("Bronze layer ingestion complete")


def process_silver_layer(**context):
    """Clean and enrich data in Silver layer."""
    print("Starting Silver Layer Processing")
    bronze_count = context['task_instance'].xcom_pull(
        task_ids='bronze_ingestion', key='bronze_row_count'
    )
    print(f"Bronze rows: {bronze_count}")
    # In production: spark-submit silver_processing.py
    print("Silver layer processing complete")


def aggregate_gold_layer(**context):
    """Create business-ready aggregations in Gold layer."""
    print("Starting Gold Layer Aggregation")
    # In production: spark-submit gold_aggregation.py
    print("Gold layer aggregation complete")


def validate_data_quality(**context):
    """Run data quality checks on Gold layer."""
    print("Running data quality checks")
    # In production: validate row counts, nulls, fraud rate range
    print("All data quality checks passed")


def send_completion_notification(**context):
    """Send pipeline completion notification."""
    print("Sending completion notification")
    # In production: Slack, email, dashboard update
    print("Notification sent")


# Task definitions
bronze_task = PythonOperator(
    task_id='bronze_ingestion',
    python_callable=ingest_bronze_layer,
    dag=dag,
)

silver_task = PythonOperator(
    task_id='silver_processing',
    python_callable=process_silver_layer,
    dag=dag,
)

gold_task = PythonOperator(
    task_id='gold_aggregation',
    python_callable=aggregate_gold_layer,
    dag=dag,
)

quality_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=validate_data_quality,
    dag=dag,
)

notify_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_completion_notification,
    dag=dag,
)


# Task dependencies
bronze_task >> silver_task >> gold_task >> quality_task >> notify_task
