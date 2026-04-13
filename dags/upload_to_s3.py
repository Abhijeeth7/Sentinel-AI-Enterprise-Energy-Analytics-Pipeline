from datetime import datetime
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.operators.bash import BashOperator

from data_producer import run_producer

default_args = {
    'owner': 'mohan',
    'start_date': datetime(2026, 4, 13),
    'retries': 1,
}

with DAG(
    dag_id='sentinel_local_to_s3',
    default_args=default_args,
    schedule='@hourly',
    catchup=False
) as dag:

    # 1. Run your existing production script
    task_generate_data = PythonOperator(
        task_id='run_data_producer',
        python_callable=run_producer
    )

    # 2. Upload the most recent file from landing_zone to S3
    task_upload_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_to_s3',
        filename="{{ ti.xcom_pull(task_ids='run_data_producer') }}",
        dest_key='raw/energy_data_{{ ts_nodash }}.json',
        dest_bucket=os.environ.get('S3_BUCKET_NAME', 'sentinel-data-lake'),
        aws_conn_id='aws_default',
        replace=True
    )


    task_dbt_run = BashOperator(
        task_id='dbt_transform_and_test',
        bash_command=(
            "dbt run --project-dir /opt/airflow/sentinel_models "
            "--profiles-dir /opt/airflow/sentinel_models && "
            "dbt test --project-dir /opt/airflow/sentinel_models "
            "--profiles-dir /opt/airflow/sentinel_models"
        )
    )

    # Assuming your tasks are named like this:
    task_generate_data >> task_upload_to_s3 >> task_dbt_run
