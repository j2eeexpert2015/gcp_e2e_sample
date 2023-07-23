import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowCreateJavaJobOperator
from airflow.utils.dates import days_ago

bucket_path = "gs://gcpsampleall/dataflow_common/all_jars/gcp_e2e_sample-1.0-SNAPSHOT.jar"
project_id = "sanguine-anthem-393416"

default_args = {
    'start_date': days_ago(1),
    'dataflow_default_options': {
        'project': project_id,
        'tempLocation': 'gs://gcpsampleall/composer/dataflow_temp',
        'runner': 'DataflowRunner'
    }
}

with models.DAG(
        'ComposerDataflowDag1',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_args) as dag:

    #t1 = DataflowJavaOperator(
    t1 = DataflowCreateJavaJobOperator(
        task_id='rundataflowjobfirst',
        jar=bucket_path,
        job_class='gcpsample.GBQDataFlowJob',
        job_name='{{task.task_id}}'
    )

    t1
