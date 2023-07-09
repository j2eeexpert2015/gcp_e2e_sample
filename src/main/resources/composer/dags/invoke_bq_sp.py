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
        'composer_to_bq_sp_invocation',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
        
    call_stored_procedure = BigQueryInsertJobOperator(
    task_id="call_stored_procedure",
    configuration={
        "query": {
            "query": "CALL `fabled-lyceum-379607.demo.populate_customer`(); ",
            "useLegacySql": False,
        }
    },
    location=location,
)

    call_stored_procedure