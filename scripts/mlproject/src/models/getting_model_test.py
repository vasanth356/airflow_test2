import pandas as pd
from utils.mlflow_class import *

# loading the config file
with open ("/home/vasanth/airflow/scripts/mlproject/config.yml", "r") as ymlfile:
 cfg = yaml.safe_load(ymlfile)

exper_name = f"name='{cfg['experiment_details']['name']}'"

def test_mlflow_predictions():

 data = pd.read_csv(cfg['location']['today_data_with_target'])
 data.drop(['quality'], axis = 1, inplace = True)
 #getting the latest version from mlflow registry
 versions = []
 for mv in client.search_model_versions(exper_name):
  versions.append(dict(mv))
 latest_version = versions[-1]['version']

 # loading the model from the model registry
 model_fetch = GettingModel(cfg['experiment_details']['name'], latest_version)
 model_mlflow = model_fetch.model()
 output = model_mlflow.predict(data)
 return output

print(test_mlflow_predictions())
