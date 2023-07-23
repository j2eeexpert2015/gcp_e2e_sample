import datetime
from airflow import models
#from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
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
        'composer_dataflow_dag',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_args) as dag:

    #t1 = DataflowJavaOperator(
    t1 = DataflowCreateJavaJobOperator(
        task_id='RunDataflowJobA',
        jar=bucket_path,
        options={
            'arguments': 'gcpsample.GBQDataFlowJob',
        },
        job_name='{{task.task_id}}',
        dataflow_default_options=default_args['dataflow_default_options']
    )

    t1
