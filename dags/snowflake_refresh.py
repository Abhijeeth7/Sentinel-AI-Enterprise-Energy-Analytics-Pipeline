from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

@dag(
    dag_id='sentinel_emergency_reconciliation',
    start_date=datetime(2026, 4, 13),
    schedule='@daily',  # Remove '_interval'
    catchup=False,
    tags=['emergency', 'sentinel']
)
def emergency_dag():

    @task
    def check_snowpipe_health():
        """
        Optional: Check if Snowpipe has been lagging.
        For a 'Junior to Startup' path, we'll keep it simple: 
        Just trigger a manual refresh to catch any missed files.
        """
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # This command forces Snowpipe to 'look again' at the S3 bucket
        # without duplicating data.
        sql = "ALTER PIPE sentinel_energy_pipe REFRESH;"
        hook.run(sql)
        print("Snowpipe refresh triggered for any missed S3 events.")

    @task
    def run_reconciliation_load():
        """
        The Emergency 'FORCE' load. 
        Only use this if you suspect files are totally missing.
        """
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # We use 'ON_ERROR = CONTINUE' so one bad JSON doesn't kill the backup.
        # We do NOT use FORCE=TRUE so we don't duplicate Snowpipe's work.
        sql = """
            COPY INTO RAW_ENERGY_DATA(json_data)
            FROM @sentinel_s3_stage
            FILE_FORMAT = (FORMAT_NAME = 'sentinel_json_format')
            ON_ERROR = 'CONTINUE';
        """
        hook.run(sql)
        print("Manual COPY executed. Snowflake metadata prevented duplicates.")

    check_snowpipe_health() >> run_reconciliation_load()

emergency_dag()