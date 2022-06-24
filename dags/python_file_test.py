from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                      datetime.min.time())

default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': seven_days_ago,
        'email': ['airflow@airflow.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
      }

def gettingcwd():
 import os
 print('getting current directory before', os.getcwd())
 os.chdir("/home/vasanth/airflow/scripts/mlproject/src/models")
 print('getting current directory after', os.getcwd())




dag = DAG('simple', default_args=default_args)
# t1 = BashOperator(
# task_id='testairflow',
# bash_command='python3 /home/vasanth/airflow/dags/scripts/file1.py',
    # dag=dag)
t2 = PythonOperator(
    task_id='testcwd',
    python_callable= gettingcwd,
    dag=dag)
