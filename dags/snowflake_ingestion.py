from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'mohan',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sentinel_snowflake_consumer',
    default_args=default_args,
    description='Manually triggers the Snowflake COPY command for S3 data reconciliation',
    schedule_interval=None, # Set to None so it only runs when YOU trigger it
    catchup=False,
    tags=['sentinel', 'snowflake', 'reconciliation']
) as dag:

    task_snowflake_load = SnowflakeOperator(
        task_id='snowflake_copy_reconciliation',
        snowflake_conn_id='snowflake_default',
        sql="""
            COPY INTO RAW_ENERGY_DATA(json_data)
            FROM @sentinel_s3_stage
            FILE_FORMAT = (FORMAT_NAME = 'sentinel_json_format')
            ON_ERROR = 'CONTINUE';
        """,
    )

    task_snowflake_load