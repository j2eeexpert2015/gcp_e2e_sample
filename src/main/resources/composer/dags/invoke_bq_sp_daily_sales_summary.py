import datetime
import time

import datetime

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryValueCheckOperator,
)

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    'start_date': yesterday
}

location="us-central1"

with models.DAG(
        #'call generate daily sales summary',
       'callthesp',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
        
    call_stored_procedure = BigQueryInsertJobOperator(
    task_id="call_generate_daily_sales_summary",
    configuration={
        "query": {
            "query": "CALL `poised-shuttle-384406.gcpsample.generate_daily_sales_summary`('2023-07-01') ",
            "useLegacySql": False,
        }
    },
    location=location,
)

    call_stored_procedure