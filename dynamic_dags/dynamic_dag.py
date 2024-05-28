import json 
import os
import airflow 
from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


CUR_DIR = os.path.abspath(os.path.dirname(__file__))

with open(f'{CUR_DIR}/config_files/dynamic_dag1.json', 'r') as config_file:
    config = json.load(config_file)['dags']

    for dag_config in config:

        if dag_config['isactive']:

            dag_id = dag_config['dag_id']
            schedule_interval = dag_config['schedule_interval']

            @dag(dag_id=dag_id, start_date=datetime(2022, 2, 1), schedule_interval=schedule_interval, default_args=default_args )
            def dynamic_generated_dag():

                START = DummyOperator(task_id  = 'START')
                END = DummyOperator(task_id  = 'END')

                for tasks_list in dag_config['tasks']:
                    
                    for task_id, task_value in tasks_list.items():
                        
                        task_id = BashOperator( task_id = task_id ,bash_command= "echo 'Output is => ' ;echo $IN", env= {"IN": task_value}, append_env=True) 
                    
                        START >> task_id >> END

        else:
            continue

        
        dynamic_generated_dag()