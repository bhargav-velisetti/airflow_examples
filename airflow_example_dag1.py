import datetime
from datetime import timedelta
import os

import airflow
from airflow import DAG
from airflow import models
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.branch_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreateJavaJobOperator
from airflow.models.xcom import XCom


default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': airflow.utils.dates.days_ago(0),
    'max_active_runs_per_dag': 1,
    'provide_context': True,
    'email': ['test@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    "dataflow_default_options": {
        "project": "my-gcp-project",
        "zone": "us-central1-f",
        "stagingLocation": "gs://bucket/tmp/dataflow/staging/",
    },
}


dag = DAG (
    "test_dag01",
    default_args=default_args,
    description='Test DAG1 for couple of airflow operators',
    max_active_runs=1,
    catchup=False, 
    dagrun_timeout=timedelta(minutes=60*1),
    schedule_interval="0 0,12,14,16,18,20,22 * * *"
    )

env_type = Variable.get('ENV', default_var='DEV')

print("Current ENV is " + env_type)


py_operator1 = PythonOperator(
  task_id='py_operator1',
  python_callable=lambda x: print("This is python operator example and value of provided args is = " + x['templates_dict']['input_param']),
  provide_context=True,
  templates_dict={'input_param': "param_value"},
  dag=dag)


bash_optr01 = BashOperator(
    task_id='bash_optr01',
    bash_command='echo "bash_optr01 output"',
)


bq_optr01 = BigQueryOperator(task_id='bq_optr01',
            sql='/tmp/1.sql',
            use_legacy_sql=False,
            dag=dag)

dummy_optr01 = DummyOperator(task_id='dummy_optr01', dag=dag)

dummy_optr02 = DummyOperator(task_id='dummy_optr02', dag=dag)

bash_optr02 = BashOperator(
    task_id='bash_optr02',
    bash_command="echo  '{{ti.xcom_pull(task_ids='bq_optr01')}}'"
)


email = EmailOperator(
        task_id='send_email',
        to='test@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>Email Test</h3> """,
        dag=dag
)


SPARK_JOB = {
    "reference": {"project_id": 'PROJECT_ID'},
    "placement": {"cluster_name": 'CLUSTER_NAME'},
    "spark_job": {
        "jar_file_uris": ["file:///path/some.jar"],
        "main_class": "org.src.main.scala.some_main_object",
    },
}

spark_job01 = DataprocSubmitJobOperator(
    task_id="spark_job", job=SPARK_JOB, region='REGION', project_id='PROJECT_ID'
)


start_template_job = DataflowTemplatedJobStartOperator(
    task_id="start-template-job",
    template='gs://dataflow-templates/latest/Word_Count',
    parameters={'inputFile': "gs://dataflow-samples/shakespeare/kinglear.txt", 'output': "gs://some_bucket/some_file.txt"}, # Arguments for Jar file goes here
    location='europe-west3',
)


task = DataflowCreateJavaJobOperator(
    gcp_conn_id="gcp_default",
    task_id="dataflow_java_job",
    jar="{{var.value.gcp_dataflow_base}}some_beam_java_snapshot-1.0.jar",
    options={
        "autoscalingAlgorithm": "BASIC",
        "maxNumWorkers": "50",
        "start": "{{ds}}",
        "partitionType": "DAY",
        "labels": {"foo": "bar"}, # Arguments for Jar file goes here
    },
    dag=dag,
)

dummy_optr01 >> [py_operator1,bash_optr01] >> dummy_optr02 >> bq_optr01 >> bash_optr02 >> email  >> spark_job01 >> start_template_job >> task 