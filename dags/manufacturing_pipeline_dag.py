import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'email': ['alerts@manufacturing-co.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2)
}

# Base command to run Spark jobs
SPARK_CMD = "python /opt/airflow/spark_jobs/{script_name}"

with DAG(
    'manufacturing_pipeline_dag',
    default_args=default_args,
    description='End-to-end lakehouse pipeline for manufacturing operations',
    schedule_interval='@daily',
    catchup=False,
    tags=['manufacturing', 'quality', 'lakehouse']
) as dag:

    # 1. Ingest raw data to Bronze
    ingest_raw_data = BashOperator(
        task_id='ingest_raw_data',
        bash_command=SPARK_CMD.format(script_name='ingest_raw.py')
    )

    # 2. Transform Bronze to Silver for Quality Metrics
    transform_quality = BashOperator(
        task_id='transform_quality',
        bash_command=SPARK_CMD.format(script_name='transform_quality.py')
    )

    # 3. Transform Bronze to Silver for Downtime Metrics
    transform_downtime = BashOperator(
        task_id='transform_downtime',
        bash_command=SPARK_CMD.format(script_name='transform_downtime.py')
    )

    # 4. Data Quality Checks on Silver Data
    def run_dq_checks():
        # Using Python operator to trigger custom checks
        import sys
        sys.path.append('/opt/airflow')
        # Placeholder for dynamic loading or custom validation call
        print("Running data quality checks on Silver layer datasets.")
        return True

    run_data_quality_checks = PythonOperator(
        task_id='run_data_quality_checks',
        python_callable=run_dq_checks
    )

    # 5. Load Silver layer to Snowflake Staging
    load_to_snowflake = BashOperator(
        task_id='load_to_snowflake',
        bash_command=SPARK_CMD.format(script_name='load_to_snowflake.py')
    )

    # Define dependencies
    ingest_raw_data >> [transform_quality, transform_downtime]
    [transform_quality, transform_downtime] >> run_data_quality_checks
    run_data_quality_checks >> load_to_snowflake
