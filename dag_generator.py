import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def dag_designer():
	base_path='/home/centos/airflow/'

	var = Variable.get('dag_config',deserialize_json = True)

	dag_name=str(var['dag'])
	schedule_interval_param=var['schedule_interval']
	commands=var['command']

	dag_template_file = open(base_path+'dag_template/dynamic_dag_template.py','r')
	dag_template = dag_template_file.read()
	dag_created_file = open(base_path+'dags/{}.py'.format(dag_name),'w+')

	dag = 'dag_id = "'+dag_name+'"'

	if(schedule_interval_param == 'None') :
		schedule_interval = 'schedule_interval = '+schedule_interval_param
	else :
		schedule_interval = 'schedule_interval = "'+schedule_interval_param+'"'


	streams=""
	task_collection=""

	if(isinstance(commands, list)):
		for i,c in enumerate(commands):
			
			task_name='task_'+str(i)
			task_string= task_name+" = SSHOperator(ssh_conn_id ='EDM_DS_EMR',task_id = '"+task_name+"',command ='"+c+ "' ,trigger_rule='all_done',dag = dag)"
			task_collection=task_collection +'\n'+ task_string
			streams= streams+ " >> "  + task_name 
		streams=streams.strip().strip(">>").strip()
	else :
		task_collection= "task = SSHOperator(ssh_conn_id ='EDM_DS_EMR',task_id = 'task',command ='"+str(commands)+ "' ,trigger_rule='all_done',dag = dag)"
		streams='task'		
	print("created Dag")
	print(streams)
	print(task_collection)

	hint="###This python code is auto generated based on the variables"

	data = hint +'\n\n\n'+dag+'\n'+schedule_interval+'\n'+dag_template+'\n'+ task_collection+'\n'+streams

	dag_created_file.write(data)
	dag_created_file.close()

dag = DAG(dag_id = 'dag_generator',start_date = datetime.today(),schedule_interval=None)
task = PythonOperator(task_id = 'create_dag_file',python_callable= dag_designer,dag=dag)

task
