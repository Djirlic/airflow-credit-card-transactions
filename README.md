# Astro Fraud Detection Orchestration with Airflow

This repository was created as part of my credit card transaction data pipeline to analyze fraud data.</br>
👉 [AWS & Snowflake Batch Processing Pipeline for Credit Card Transactions](https://github.com/Djirlic/fraud-detection-e2e-pipeline). 

1. Data is ingested into an S3 bucket, validated, moved to another refined bucket.
1. Via AWS EventBridge, SNS, and SQS, Snowpipe gets triggered and loads the data automatically.
1. **Airflow detects new files via a sensor operator (described below).**
1. If new data is available, dbt gets triggered and runs transforms the raw data with multiple models to gold layer use case ready tables.
1. Steamlit consumes and visualizes this data.

## ⏭️ DAG: Snowflake Stream Sensor

_snowflake_update_sensor.py_

This DAG monitors the Snowflake stream TRANSACTIONS_RAW_STREAM for new credit card transactions.
When new rows arrive, it triggers the dbt Cloud job to process data through the Bronze → Silver → Gold layers.

Workflow

1.	Sensor – Checks every 10 minutes if the stream has new rows via SYSTEM$STREAM_HAS_DATA.
1.	dbt Trigger – Runs the dbt job in Cloud using the DbtCloudRunJobOperator.
1.	Advance Offset – Executes a no-op insert into a temporary table.
    - This safely advances the stream offset to the current version of the table.
	- Without this step, the stream would continue reporting the same unconsumed rows, and Airflow would re-run dbt unnecessarily.
	- To achieve this, we query the stream but filter out all rows (WHERE 1=0), effectively consuming metadata only.


## ⚒️ Other information

- The dbt job ID is stored as an Airflow Variable (secret_dbt_job_id) to keep sensitive values out of source control.
- Connections to Snowflake (snowflake_to_airflow_conn) and dbt Cloud (airflow_to_dbt) must be configured in the Airflow UI.