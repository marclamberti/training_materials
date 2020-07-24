from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

import wget
from datetime import datetime

def download_dataset():
	url = 'https://raw.githubusercontent.com/marclamberti/training_materials/master/data/avocado.csv'
	wget.download(url, out='/usr/local/airflow/avocado.csv')

dag = DAG('avocado_dag', description='Forecasting avocado prices', schedule_interval='*/10 * * * *', start_date=datetime(2020, 1, 1), catchup=False)

task_1 = DummyOperator(task_id='task_1', retries=3, email_on_retry=False, dag=dag)
task_2 = DummyOperator(task_id='task_2', retries=3, email_on_retry=False, dag=dag)
	