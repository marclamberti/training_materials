from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from time import sleep
from datetime import datetime

def print_hello():
	sleep(5)
	return 'Hello World'

dag = DAG('dag', description='First DAG', schedule_interval='*/10 * * * *', start_date=datetime(2020, 1, 1), catchup=False)

dummy_task 	= DummyOperator(task_id='dummy_task', retries=3, email_on_failure=False, dag=dag)
python_task	= PythonOperator(task_id='python_task', retries=3, email_on_failure=False, python_callable=print_hello, dag=dag)

dummy_task.set_downstream(python_task)