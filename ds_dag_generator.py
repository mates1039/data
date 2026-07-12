import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def dag_designer():
    base_path='/home/centos/airflow/'
    var = Variable.get('emr_notebook_config',deserialize_json = True)
        
    ip_url=str(var['ulr'])
    schedule_interval_param=var['schedule_interval']
    dag_name=str(var['dag'])
    
    dag_template_file = open(base_path+'dag_template/ds_dynamic_dag_template.py','r')
    dag_template = dag_template_file.read()
    dag_created_file = open(base_path+'dags/{}.py'.format(dag_name),'w+')

    dag = 'dag_id = "'+dag_name+'"'
    ip = 'ip = """'+ip_url+'"""'
    if(schedule_interval_param == 'None') :
        schedule_interval = 'schedule_interval = '+schedule_interval_param
    else :
        schedule_interval = 'schedule_interval = "'+schedule_interval_param+'"'

    hint="###This python code is auto generated based on the variables"

    data = hint +'\n\n\n'+dag+'\n'+schedule_interval+'\n'+ip+'\n'+ dag_template 
    
    dag_created_file.write(data)
    dag_created_file.close()

dag = DAG(dag_id = 'ds_dag_generator',start_date = datetime.today(),schedule_interval=None)
task = PythonOperator(task_id = 'create_dag_file',python_callable= dag_designer,dag=dag)

task
   
