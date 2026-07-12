dag_id = 'Treasury_DataTape_Daily'
schedule_interval = "00 13 * * 1-5"
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta, time
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.models.xcom import XCom
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowSkipException
from airflow.exceptions import AirflowException

import time as sleepvar
import datetime as dt
import psycopg2
import configparser

def IntervalCheck():
    if(int(datetime.now().strftime("%H%M")) > int('2230')):
        command = """python3 /opt/airflow/config/send_email.py 'datawarehousealerts@purchasingpower.com' 'datateamnotification@purchasingpower.com,jbedi@purchasingpower.com,harimurugesan@purchasingpower.com,pmanning@purchasingpower.com' 'Daily Treasury Data Failed To Load' 'Preload Diff has not matched or data did not load yet. \n Shutting down the job. \n Date - '$(date +'%m-%d-%Y')''"""
    else:
        command = """ pwd """
    return BashOperator(task_id='Loop',bash_command=command)
def raiseExcep():
    if(int(datetime.now().strftime("%H%M")) > int('2230') or int(datetime.today().weekday())==5 or int(datetime.today().weekday())==6):
        raise AirflowSkipException("Preload Diff has not matched or data didn't load yet. \n Shutting down the job. \n")
def clear_upstream_task(context):
    execution_date = context['execution_date']
    bash_command='airflow tasks clear -s {}  -t Loop -d -y Treasury_DataTape_Daily'.format(execution_date)
    clear_tasks = BashOperator(
        task_id='clear_tasks',
        bash_command=bash_command
    )
    return clear_tasks.execute(context=context)
def GetStatus(sql):
    pg_hook = PostgresHook(postgres_conn_id='EDDPROD_GP')
    status = pg_hook.get_records(sql=sql)
    print(status)
    return_value = str(status)
    finalvalue = ''.join(filter(lambda i: i.isdigit() or i is '.', return_value))
    if(str((datetime.now()-dt.timedelta(days=1)).strftime("%Y%m%d"))) == str(finalvalue):
        return finalvalue
    else:
        print("Last data was loaded on {}. Either preload difference is not in range or data is yet to come. \n Sleeping for 15 minutes........................".format(finalvalue))
        sleepvar.sleep(600)
        #if(int(datetime.now().strftime("%H%M")) > int('0400')):
        #    raise AirflowSkipException("Preload Diff has not matched or data didn't load yet. \n Shutting down the job. \n")
        print( "The data is not loaded yet. Retrying..........")
        return restart
def TreasuryDataTapeFunction():
    query_exec = "select to_char(max(as_of_date),'YYYYMMDD') from ods.treasury_datatape_daily tdd;"
    return PythonOperator(
                task_id='check_status',
                python_callable=GetStatus,
                retries=40,
                op_kwargs={
                        'sql': query_exec
                },
                on_retry_callback=clear_upstream_task,
                trigger_rule='all_success',
                dag=dag
        )
def GetResult(**kwargs):
    value = "{{task_instance.xcom_pull(task_ids='check_status')}}"
    command = """python3 /opt/airflow/config/send_email.py 'datawarehousealerts@purchasingpower.com' 'datateamnotification@purchasingpower.com,jbedi@purchasingpower.com,harimurugesan@purchasingpower.com,treasury@purchasingpower.com,pmanning@purchasingpower.com,ggorga@purchasingpower.com' 'Daily Treasury Data Loaded' 'Daily Treasuy Data has been loaded on '$(date +'%m-%d-%Y')''"""
    return BashOperator(task_id='SendMail',bash_command=command)

default_args = {
    'owner': 'EDM_Validation',
    'depends_on_past': False,
    'email': ['datateamnotification@purchasingpower.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    }

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    start_date=datetime(2025,8,7),
    schedule_interval=schedule_interval,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    dagrun_timeout=timedelta(seconds=28800),
    ) as dag:
    task_0 = IntervalCheck()
    task_1 = PythonOperator(task_id='initialvar',python_callable= raiseExcep,dag=dag,)
    #task_2 = PostgresOperator(task_id="RunningFunc",postgres_conn_id="EDDPROD_GP",sql="SELECT ods.treasury_datatape_daily_fn();",dag=dag)
    task_2 = SSHOperator(ssh_conn_id ='EDM_EMR',task_id = 'RunningFunc',command ='sh /mnt/usr/local/src/batch/ipynb_trigger/./runJob.sh "s3://ppc-edm-prod-ds/studio/e-BLF7HJSHQTMI9GT5SOG7MTU31/Production_Pyspark/exec/ODS/NLS_Daily_DataTape.ipynb"' ,trigger_rule='all_done',cmd_timeout=3600,dag = dag)
    task_3 = TreasuryDataTapeFunction()
    task_4 = GetResult()
    task_0 >> task_1 >> task_2 >> task_3 >> task_4
