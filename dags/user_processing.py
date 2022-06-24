# importing the dag object from the airflow library
from airflow.models import DAG
# importing the sqlite object from airflow library
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
# importing the Http sensor
from airflow.providers.http.sensors.http import HttpSensor
#importing the simple http operator
from airflow.providers.http.operators.http import SimpleHttpOperator
#importing json library
import json
#importing the python operator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# importing the json deserializer
# from pandas import json_normalize
from pandas.io.json import json_normalize

def _processing_user(ti):
  users = ti.xcom_pull(task_ids=['extracting_user'])
  if not len(users) or 'results' not in users[0]:
   raise ValueError('user is Empty')
  user = users[0]['results'][0]
  processed_user = json_normalize ( {
      'firstname' : user['name']['first'],
      'lastname' : user['name']['last'],
       'country' : user['location']['country'],
       'username' : user['login']['username'],
       'password': user['login']['password'],
        'email' : user['email']
       } )
  processed_user.to_csv('/tmp/processed_user.csv',index = None, header =False)



# importing the datetime mdule from python
from datetime import datetime


# default arguments to use it in the tasks of the dags
default_args ={
'start_date' : datetime(2020,1,1)
 }

# instinate the object of dag
# dag is should be unique in the dag folder below example 'user_processing' is the name
with DAG('user_processing', schedule_interval = '@daily',
            default_args = default_args,
            catchup = False) as dag:

# define tasks/operators

# one opeartor one task

# three opeartors
# action operators : Execute an action
# Transfer operators : Transfer data
# sensors: wait for condition to be met


    creating_table = SqliteOperator(task_id = 'creating_table',sqlite_conn_id = 'db_sqlite',
                     sql = '''
                        
                        CREATE TABLE IF NOT EXISTS users (
                        firstname TEXT NOT NULL,
                        lastname TEXT NOT NULL,
                        country TEXT NOT NULL,
                        username TEXT NOT NULL,
                        password TEXT NOT NULL,
                        email TEXT NOT NULL PRIMARY KEY
                        );
                        '''
    )

    is_api_available = HttpSensor(
                        task_id = 'is_api_available',
                        http_conn_id = 'user_api',
                        endpoint = 'api/'
                       )

    extracting_user = SimpleHttpOperator(
                        task_id='extracting_user',
                        http_conn_id='user_api',
                        endpoint='api/',
                        method='GET',
                        response_filter=lambda response: json.loads(response.text),
                        log_response=True
                      )
    processing_user = PythonOperator(
                       task_id = 'processing_user',
                       python_callable = _processing_user
                       )
    storing_user = BashOperator(task_id='storing_user',
                    bash_command='echo -e ".separator ","\n.import /tmp/processed_user.csv users" | sqlite3 /home/vasanth/airflow/airflow.db'
                     )
    creating_table >> is_api_available >> extracting_user >> processing_user >> storing_user
