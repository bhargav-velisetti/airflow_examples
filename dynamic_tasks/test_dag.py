import airflow 
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('POCDAG',default_args= default_args  , schedule_interval= '1 1 * * *')


'''
START >> A >> B >> C >> D >> END 
B >> E >> END 
[A,B,C] >> F >> END 
'''
tasks_dict = {}

START = DummyOperator(task_id  = 'START', dag = dag)
END = DummyOperator(task_id  = 'END', dag = dag)

tasks_dict.update( { 'START' : START, 'END' : END } )


def return_bash_task(PLACEHOLDER : str):

    return BashOperator( task_id = PLACEHOLDER ,bash_command= "echo 'Output is => ' ;echo $IN", env= {"IN": PLACEHOLDER}, append_env=True , dag = dag)


import json 
import os

CUR_DIR = os.path.abspath(os.path.dirname(__file__))

with open(f'{CUR_DIR}/tasks_config.json', 'r') as config_file:
    config = json.load(config_file)['tasks']

    for task in config:
        task_id = task['current']
        tasks_dict.update( {task_id : return_bash_task(task_id)} )


    for task in config:

        pre_ = task['pre'] 
        cu =  tasks_dict[ task['current'] ]
        next = tasks_dict[ task['next'] ] 

        if isinstance(pre_, list):

            pre_task = []

            for i in pre_ :
                pre_task.append( tasks_dict.get(i) )

        else:
            pre_task = tasks_dict[ pre_ ]
            
        pre_task >> cu >> next 

    

