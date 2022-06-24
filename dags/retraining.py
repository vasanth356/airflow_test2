from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

default_args = {
                 'start_date' : datetime(2022,1,1)
                  }
def _retraining(ti):
    print('checking for the retraining')
    result = ti.xcom_pull(key='return_value', task_ids = [ 'monitoring'] )
    print('result is ', result)
    if result[0] == 'drifted':
     return ('retraining_required')
    else:
     return ('retraining_not_required')

def _comparsion_of_models(ti):
    print('comparsion check')
    result = ti.xcom_pull(key='return_value', task_ids = ['new_model_evaluation'] )
    print('result is ', result)
    if result[0] == 'new is better':
     return ('saving_retrained_model')
    else:
     return ('deleting_retrained_model')

with DAG('retraining', schedule_interval = '@daily', default_args = default_args, catchup =False) as dag:

   data_preprocessing = BashOperator(
              task_id = 'data_preprocessing',
              bash_command='python3 /home/vasanth/airflow/scripts/mlproject/src/data/preprocessing.py',
              do_xcom_push=False
               )
   daily_prediction = BashOperator(
              task_id = 'daily_prediction',
              bash_command='python3 /home/vasanth/airflow/scripts/mlproject/src/models/predictions.py',
              do_xcom_push=False
               )
   yesterdays_prediction = BashOperator(
              task_id = 'yesterdays_prediction',
              bash_command='python3 /home/vasanth/airflow/scripts/mlproject/src/models/yesterday_predictions.py',
              do_xcom_push=False
               )
   monitoring = BashOperator(
              task_id = 'monitoring',
              bash_command='python3 /home/vasanth/airflow/scripts/mlproject/src/models/monitoring.py',
              do_xcom_push=True
               )
   retraining_check = BranchPythonOperator(
                       task_id = 'retraining_check',
                       python_callable = _retraining
                       )

   retraining_required = DummyOperator (
                    task_id = 'retraining_required'
                     )
   retraining_not_required = DummyOperator(
                             task_id = 'retraining_not_required'
                               )
   retraining = BashOperator(
              task_id = 'retraining',
              bash_command='python3 /home/vasanth/airflow/scripts/mlproject/src/models/retraining.py',
              do_xcom_push=False
               )
   new_model_evaluation = BashOperator(
              task_id = 'new_model_evaluation',
              bash_command='python3 /home/vasanth/airflow/scripts/mlproject/src/models/retrain_evaluation.py',
              do_xcom_push=True
               )

   model_comparsion_to_old_model = BranchPythonOperator(
                       task_id = 'model_comparsion_to_old_model',
                       python_callable = _comparsion_of_models
                       )

   saving_retrained_model = BashOperator(
              task_id = 'saving_retrained_model',
              bash_command='cp /home/vasanth/airflow/scripts/mlproject/models/trained_rf.pkl /home/vasanth/airflow/scripts/mlproject/models/rf.pkl',
              do_xcom_push=False
               )
   deleting_retrained_model = BashOperator(
              task_id = 'deleting_retrained_model',
              bash_command='rm /home/vasanth/airflow/scripts/mlproject/models/trained_rf.pkl',
              do_xcom_push=False
               )
   daily_prediction_from_retrained_model = BashOperator(
              task_id = 'daily_prediction_from_retrained_model',
              bash_command='python3 /home/vasanth/airflow/scripts/mlproject/src/models/predictions.py',
              do_xcom_push=False
               )


   data_preprocessing >> daily_prediction >> yesterdays_prediction >> monitoring >> retraining_check

   retraining_check >>  [retraining_required,retraining_not_required]
   
   retraining_required >> retraining >> new_model_evaluation >> model_comparsion_to_old_model >> [saving_retrained_model,deleting_retrained_model]
   
   saving_retrained_model >> daily_prediction_from_retrained_model
