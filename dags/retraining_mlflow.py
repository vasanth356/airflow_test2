from airflow.utils.trigger_rule import TriggerRule
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

def _model_selection():
    training = 'model_with_new_parameters'
    if  training == 'model_with_new_parameters':
     return ('retraining_with_new_parameters')
    elif training == 'retraining_with_new_model':
     return ('retraining_with_new_model')

with DAG('retraining_mlflow', schedule_interval = '@daily', default_args = default_args, catchup =False) as dag:

   data_preprocessing = BashOperator(
              task_id = 'data_preprocessing',
              bash_command='python3 /home/vasanth/airflow/scripts/mlproject/src/data/preprocessing.py',
              do_xcom_push=False
               )
   unit_testing = BashOperator(
              task_id = 'unit_testing',
              bash_command='python3 -m pytest /home/vasanth/airflow/scripts/mlproject/src/models/test_ml_unit.py'
               )
   daily_prediction = BashOperator(
              task_id = 'daily_prediction',
              bash_command='python3 /home/vasanth/airflow/scripts/mlproject/src/models/mlflow_predictions.py',
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
   retraining_method = BranchPythonOperator(
                       task_id = 'retraining_method',
                       python_callable = _model_selection
                       )
   retraining_with_new_parameters = BashOperator(
              task_id = 'retraining_with_new_parameters',
              bash_command='python3 /home/vasanth/airflow/scripts/mlproject/src/models/train_mlflow.py',
              do_xcom_push=False
               )
   retraining_with_new_model = BashOperator(
              task_id = 'retraining_with_new_model',
              bash_command='python3 /home/vasanth/airflow/scripts/mlproject/src/models/train_new_model_mlflow.py',
              do_xcom_push=False
               )
   new_model_evaluation = BashOperator(
              task_id = 'new_model_evaluation',
              bash_command='python3 /home/vasanth/airflow/scripts/mlproject/src/models/retrain_evaluation.py',
              do_xcom_push=True,
              trigger_rule=TriggerRule.ONE_SUCCESS
               )

   model_comparsion_to_old_model = BranchPythonOperator(
                       task_id = 'model_comparsion_to_old_model',
                       python_callable = _comparsion_of_models
                       )

   saving_retrained_model = BashOperator(
              task_id = 'saving_retrained_model',
              bash_command='python3  /home/vasanth/airflow/scripts/mlproject/src/models/model_transistion_production.py',
              do_xcom_push=False
               )
   deleting_retrained_model = BashOperator(
              task_id = 'deleting_retrained_model',
              bash_command='python3  /home/vasanth/airflow/scripts/mlproject/src/models/model_transistion_archive.py',
              do_xcom_push=False
               )
   daily_prediction_from_retrained_model = BashOperator(
              task_id = 'daily_prediction_from_retrained_model',
              bash_command='python3 /home/vasanth/airflow/scripts/mlproject/src/models/mlflow_predictions.py',
              do_xcom_push=False
               )


   data_preprocessing >> unit_testing >> daily_prediction >> yesterdays_prediction >> monitoring >> retraining_check

   retraining_check >>  [retraining_required,retraining_not_required]

   retraining_required >> retraining_method >> [retraining_with_new_parameters, retraining_with_new_model]

   [retraining_with_new_parameters, retraining_with_new_model] >> new_model_evaluation >> model_comparsion_to_old_model >> [saving_retrained_model,deleting_retrained_model]

   # retraining_with_new_model>> new_model_evaluation >> model_comparsion_to_old_model >> [saving_retrained_model,deleting_retrained_model]

   saving_retrained_model >> daily_prediction_from_retrained_model
