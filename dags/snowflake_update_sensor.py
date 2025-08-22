from airflow.sdk import Variable
from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime

import logging

task_logger = logging.getLogger("airflow.task")
STREAM_NAME = "TRANSACTIONS_RAW_STREAM"

@dag(
    start_date=datetime(2025, 8, 5),
    schedule="@daily",
    catchup=False,
    tags=["snowflake", "sensor"],
)
def check_new_cc_transactions_data_sensor():
    task_logger.info("✅ DAG check_new_cc_transactions_data_sensor initialized")
    
    @task.sensor(
        poke_interval=600,
        timeout=86400,
        mode="reschedule"
    )
    def check_new_data() -> PokeReturnValue:
        task_logger.info("⚙️ Run check_new_data.")
        
        hook = SnowflakeHook(snowflake_conn_id="snowflake_to_airflow_conn")
        task_logger.info("[Sensor] Connection retrieved.")
        
        (has_data_raw,) = hook.get_first(f"SELECT SYSTEM$STREAM_HAS_DATA('{STREAM_NAME}')")
        task_logger.info("[Sensor] Result received from SQL.")
        
        has_new_rows = str(has_data_raw).upper() == "TRUE"
        task_logger.info(f"[Sensor] Has new rows {STREAM_NAME}: {has_new_rows}")
        
        return PokeReturnValue(is_done=has_new_rows)

    run_dbt_job = DbtCloudRunJobOperator(
            task_id="trigger_dbt_job",
            dbt_cloud_conn_id="airflow_to_dbt",
            job_id=Variable.get("secret_dbt_job_id"),
            check_interval=10,
            timeout=300,
        )
    
    advance_stream = SQLExecuteQueryOperator(
        task_id="advance_stream_noop",
        conn_id="snowflake_to_airflow_conn",
        sql="""
        CREATE TEMP TABLE IF NOT EXISTS CC_TRANSACTIONS_DB.RAW.TMP_CONSUME_STREAM AS
        SELECT * FROM CC_TRANSACTIONS_DB.RAW.TRANSACTIONS_RAW WHERE 1=0;

        INSERT INTO CC_TRANSACTIONS_DB.RAW.TMP_CONSUME_STREAM
        SELECT * EXCLUDE (METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID)
        FROM CC_TRANSACTIONS_DB.RAW.TRANSACTIONS_RAW_STREAM
        WHERE 1=0;
        """,
    )

    check_new_data() >> run_dbt_job >> advance_stream

check_new_cc_transactions_data_sensor()