from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from datetime import datetime

import logging

task_logger = logging.getLogger("airflow.taask")

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
        task_logger.info("⚙️ Run check_new_cc_transactions_data_sensor: Will return True for is_done.")
        return PokeReturnValue(is_done=True)
    
    check_new_data()

check_new_cc_transactions_data_sensor()