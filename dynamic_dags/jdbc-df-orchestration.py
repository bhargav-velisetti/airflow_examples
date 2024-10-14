import os
import datetime
import airflow
import json
import uuid
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud.bigquery.client import RowIterator
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator, DataflowTemplatedJobStartOperator


default_args = {}

CUR_DIR = os.path.abspath(os.path.dirname(__file__))


with open(f'{CUR_DIR}/config.json') as f:
        CONF : dict = json.load(f)


def gen_run_id():
            # short uuid 
            return str(uuid.uuid4())[:8]

def get_tr_status(TR):
        if TR == "YES":
            return "true"
        else:
            return "false"


def get_last_chk_val(bq_checkpoint_table, src_delta_col_type):
    QUERY = ( "SELECT cast(extract_start_tsp as {src_delta_col_type}) as extract_start_tsp FROM `{bq_checkpoint_table}`".format(src_delta_col_type=src_delta_col_type,bq_checkpoint_table=bq_checkpoint_table) )
    print(QUERY)
    client = bigquery.Client()
    query_job = client.query(QUERY)  
    for row in query_job.result():
        extract_start_tsp = row.get('extract_start_tsp')
    return extract_start_tsp.strftime("%Y-%m-%d %H:%M:%S")


def get_tables(CONF):
    client = bigquery.Client()
    QUERY = ( 'SELECT * FROM `{}`'.format(CONF.get('config_table')) )
    query_job = client.query(QUERY)  
    rows = query_job.result()
    return rows



def build_ingestion_dag(CONF : dict):

    short_uuid = gen_run_id()

    #Building the DAG 
    with airflow.DAG('df_jdbc_example',catchup=False,default_args=default_args, schedule_interval='1 1 * * *') as dag:

        d1 = DummyOperator(task_id='start')
        d2 = DummyOperator(task_id='stop')

        rows = get_tables(CONF)

        for row in rows:
            src_jdbc_type :str           = row.get('src_jdbc_type')
            src_jdbc_conn :str           = row.get('src_jdbc_conn')
            src_table :str               = row.get('src_table')
            src_delta_column :str        = row.get('src_delta_column')
            src_delta_col_type :str      = row.get('src_delta_col_type')
            bq_stg_project :str          = row.get('bq_stg_project')
            bq_stg_schema :str           = row.get('bq_stg_schema')
            bq_stg_table :str            = row.get('bq_stg_table')
            bq_edw_project :str          = row.get('bq_edw_project')
            bq_edw_schema :str           = row.get('bq_edw_schema')
            bq_edw_table :str            = row.get('bq_edw_table')
            bq_checkpoint_table :str     = row.get('bq_checkpoint_table')
            jdbc_driver_gcs_path :str    = row.get('jdbc_driver_gcs_path')
            TR  :str                     =  row.get('TR')
            isenabled :str               = row.get('isenabled')

            # will build add tasks if table is enabled.
            if str(isenabled).upper() == 'YES':
                print(TR)
                print(src_table)

                #Building the extraction query based on truncate and reload option
                if TR.upper() == 'YES':
                    extraction_query = "select * from {};".format(src_table)
                elif TR.upper() == 'NO' and src_jdbc_type == 'sqlserver':
                    last_chk_val = get_last_chk_val(bq_checkpoint_table, src_delta_col_type)
                    extraction_query = "select * from {src_table} where {src_delta_column} >= '{last_chk_val}';".format(src_table=src_table, src_delta_column=src_delta_column, last_chk_val=last_chk_val )
                else:
                    print(src_table + " mandatory data is not inserted into config table ")

                
                #Dataflow job options
                parameters = {
                    "connectionURL" :   CONF.get('sqlserver').get('connectionURL'), 
                    "username" : CONF.get('sqlserver').get('username'),
                    "password" : CONF.get('sqlserver').get('password'),
                    "driverJars" : CONF.get('sqlserver').get('driverJars'),
                    "driverClassName" : CONF.get('sqlserver').get('driverClassName'),
                    "fetchSize" : CONF.get('sqlserver').get('fetchSize'),
                    "outputTable" : "{}:{}.{}".format(bq_stg_project,bq_stg_schema,bq_stg_table),
                    "isTruncate" : get_tr_status(TR.upper()),
                    "query" : extraction_query,
                    "bigQueryLoadingTemporaryDirectory": CONF.get('bigquery').get('bigQueryLoadingTemporaryDirectory'),
                    "createDisposition" : CONF.get('bigquery').get('createDisposition'),
                    "useStorageWriteApi" : CONF.get('bigquery').get('useStorageWriteApi'),
                    "useStorageWriteApiAtLeastOnce" : CONF.get('bigquery').get('useStorageWriteApiAtLeastOnce'),
                    "KMSEncryptionKey" : CONF.get('KMSEncryptionKey')
                    }
                
                environment =  {
                    "numWorkers": CONF.get('dataflow').get('num-workers'),
                    "maxWorkers": CONF.get('dataflow').get('max-workers'),
                    "tempLocation": CONF.get('dataflow').get('temp-location'),
                    "network": CONF.get('dataflow').get('network'),
                    "subnetwork": CONF.get('dataflow').get('subnetwork'),
                    "workerZone": CONF.get('dataflow').get('worker-zone'),
                    "stagingLocation": CONF.get('dataflow').get('staging-location'),
                    }

                body = {
                        "launchParameter": {
                            "jobName": "df-jdbcingest-{}-{}".format(bq_stg_table,short_uuid).lower().replace('_', '-'),
                            "parameters": parameters,
                            "containerSpecGcsPath": CONF.get('dataflow').get('template'),
                            "environment": environment
                        }
                }


                start_ingest_template_job = DataflowStartFlexTemplateOperator(
                task_id= f"start_ingest_template_job_{src_table}".lower(),
                project_id=CONF.get('dataflow').get('project'),
                location=CONF.get('dataflow').get('location'),
                body=body,
                dag=dag
                )

                d1 >> start_ingest_template_job
                #No Reason to run the mtlz, if the table is noted as truncate and reload.
                if TR.upper() == 'YES':
                    start_ingest_template_job >> d2 
                else:

                    start_mtlz_template_job = BigQueryInsertJobOperator(
                    task_id=f"start_mtlz_template_job_{src_table}".lower(),
                    configuration={
                    "query":{
                        "query": "some merge sql specific to the table",
                        "use_legacy_sql":False,
                    }
                    },
                    location="US",
                    dag=dag 
                    )

                    start_ingest_template_job >> start_mtlz_template_job >> d2 
        
build_ingestion_dag(CONF=CONF)
