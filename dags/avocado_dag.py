from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

import urllib.request
from datetime import datetime

def download_dataset():
	url = 'https://raw.githubusercontent.com/marclamberti/training_materials/master/data/avocado.csv'
	urllib.request.urlretrieve(url, filename='/tmp/avocado.csv')

def read_rmse():
    accuracy = 0
    with open('/tmp/out-model-avocado-prediction-rmse.txt') as f:
        accuracy = float(f.readline())
    return 'accurate' if accuracy < 0.15 else 'inaccurate'

dag = DAG('avocado_dag', description='Forecasting avocado prices', schedule_interval='*/10 * * * *', start_date=datetime(2020, 1, 1), catchup=False)

task_1 = DummyOperator(task_id='task_1', retries=3, email_on_retry=False, dag=dag)
task_2 = DummyOperator(task_id='task_2', retries=3, email_on_retry=False, dag=dag)
	