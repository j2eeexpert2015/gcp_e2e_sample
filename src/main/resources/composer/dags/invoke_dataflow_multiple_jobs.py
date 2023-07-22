import datetime
from airflow import models
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.utils.dates import days_ago

bucket_path = "gs://your-bucket/yourDataflowJar.jar"
project_id = "your-project-id"

default_args = {
    'start_date': days_ago(1),
    'dataflow_default_options': {
        'project': project_id,
        'tempLocation': 'gs://your-bucket/tmp',
        'runner': 'DataflowRunner'
    }
}

with models.DAG(
        'composer_dataflow_dag',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_args) as dag:

    t1 = DataflowJavaOperator(
        task_id='run_dataflow_java_A',
        jar=bucket_path,
        options={
            'arguments': 'com.example.A',
        },
        job_name='{{task.task_id}}',
        dataflow_default_options=default_args['dataflow_default_options']
    )

    t2 = DataflowJavaOperator(
        task_id='run_dataflow_java_B',
        jar=bucket_path,
        options={
            'arguments': 'com.example.B',
        },
        job_name='{{task.task_id}}',
        dataflow_default_options=default_args['dataflow_default_options']
    )

    t1 >> t2
