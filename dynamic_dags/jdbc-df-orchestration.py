


from google.cloud import bigquery
from google.cloud.bigquery.client import RowIterator
from datetime import datetime
import uuid




with open('./config.json') as f:
    CONF : dict = json.load(f)


def gen_run_id():
        # short uuid 
        return str(uuid.uuid4())[:8]

def get_tr_status(TR):
    if TR == "YES":
        return "true"
    else:
        return "false"


def get_last_chk_val(bq_cntrl_table, src_delta_col_type):

    QUERY = ( "SELECT cast(extract_start_tsp as {src_delta_col_type}) as extract_start_tsp FROM `{bq_cntrl_table}`".format(src_delta_col_type=src_delta_col_type,bq_cntrl_table=bq_cntrl_table) )

    print(QUERY)

    client = bigquery.Client()

    query_job = client.query(QUERY)  

    for row in query_job.result():
        extract_start_tsp = row.get('extract_start_tsp')

    client.close()

    return extract_start_tsp.strftime("%Y-%m-%d %H:%M:%S")
 

def build_ingestion_dag(CONF : dict) -> RowIterator:

    client = bigquery.Client()
    
    QUERY = ( 'SELECT * FROM `{}`'.format(CONF.get('config_table')) )
    
    query_job = client.query(QUERY)  

    rows = query_job.result()

    client.close()


    short_uuid = gen_run_id()


    for row in rows:
        src_jdbc_type :str       = row.get('src_jdbc_type')
        src_jdbc_conn :str       = row.get('src_jdbc_conn')
        src_table :str           = row.get('src_table')
        src_delta_column :str    = row.get('src_delta_column')
        src_delta_col_type :str  = row.get('src_delta_col_type')
        bq_stg_project :str      = row.get('bq_stg_project')
        bq_stg_schema :str       = row.get('bq_stg_schema')
        bq_stg_table :str        = row.get('bq_stg_table')
        bq_core_project :str     = row.get('bq_core_project')
        bq_core_schema :str      = row.get('bq_core_schema')
        bq_core_table :str       = row.get('bq_core_table')
        bq_cntrl_table :str      = row.get('bq_cntrl_table')
        jdbc_driver_gcs_path :str = row.get('jdbc_driver_gcs_path')
        TR  :str                 =  row.get('TR')
        isenabled :str           = row.get('isenabled')


        print(TR)

        print(src_table)

        if TR.upper() == 'YES':
            extraction_query = "select * from {} ;".format(src_table)
        elif TR.upper() == 'NO' and src_jdbc_type == 'sqlserver':
            last_chk_val = get_last_chk_val(bq_cntrl_table, src_delta_col_type)
            extraction_query = "select * from {src_table} where {src_delta_column} >= '{last_chk_val}' ;".format(src_table=src_table, src_delta_column=src_delta_column, last_chk_val=last_chk_val )
        else:
            print(src_table + " mandatory data is not inserted into config table ")

        
        print(extraction_query)


        BODY = {
            "launchParameter": {
                "jobName": "df-jdbcingest-{}-{}".format(bq_stg_table,short_uuid),
                "parameters": {
                        "connectionURL" :   CONF.get('sqlserver').get('connectionURL'),
                        "driverJars" : CONF.get('sqlserver').get('driverJars'),
                        "driverClassName" : CONF.get('sqlserver').get('driverClassName'),
                        "fetchSize" : CONF.get('sqlserver').get('fetchSize'),
                        "outputTable" : "{}:{}.{}".format(bq_stg_project,bq_stg_schema,bq_stg_table),
                        "isTruncate" : get_tr_status(TR.upper()),
                        "query" : extraction_query

                },
                "environment": {
                    {
                        "numWorkers": CONF.get('dataflow').get('num-workers'),
                        "maxWorkers": CONF.get('dataflow').get('max-workers'),
                        "zone": CONF.get('dataflow').get('location'),
                        "tempLocation": CONF.get('dataflow').get('temp-location'),
                        "network": CONF.get('dataflow').get('network'),
                        "subnetwork": CONF.get('dataflow').get('subnetwork'),
                        "kmsKeyName": CONF.get('KMSEncryptionKey'),
                        "workerRegion": CONF.get('dataflow').get('location'),
                        "workerZone": CONF.get('dataflow').get('worker-zone'),
                        "stagingLocation": CONF.get('dataflow').get('staging-location'),
                    }
                },
                "containerSpecGcsPath": CONF.get('dataflow').get('template'),
            }
        }

        
        start_template_job = DataflowTemplatedJobStartOperator(
        task_id="start_template_job",
        project_id=CONF.get('dataflow').get('project'),
        template=CONF.get('dataflow').get('template'),
        location=CONF.get('dataflow').get('location'),
        body=BODY,
        )
        




build_ingestion_dag(CONF=CONF)


    
