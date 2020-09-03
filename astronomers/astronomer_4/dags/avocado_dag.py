from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.subdag_operator import SubDagOperator
from include.helpers.astro import download_dataset, read_rmse, check_dataset
from include.subdags.training_subdag import subdag_factory
from airflow.operators.notebook_plugin import NotebookToKeepOperator, NotebookToGitOperator

from datetime import datetime, timedelta

default_args = {
    'retries': 3,
    'email_on_retry': False,
    'email_on_failure': False,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1)
}

with DAG('avocado_dag', 
    default_args=default_args, 
    description='Forecasting avocado prices', 
    schedule_interval='*/10 * * * *',  
    catchup=False) as dag:

    creating_accuray_table = PostgresOperator(
        task_id='creating_accuray_table',
        sql='sql/CREATE_TABLE_ACCURACIES.sql',
        postgres_conn_id='postgres'
    )

    downloading_data = PythonOperator(
        task_id='downloading_data',
        python_callable=download_dataset
    )
	
    sanity_check = PythonOperator(
        task_id='sanity_check',
        python_callable=check_dataset,
        provide_context=True
    )

    waiting_for_data = FileSensor(
        task_id='waiting_for_data',
        fs_conn_id='fs_default',
        filepath='avocado.csv',
        poke_interval=15
    )

    training_model_tasks = SubDagOperator(
        task_id='training_model_tasks',
        subdag=subdag_factory('avocado_dag', 'training_model_tasks', default_args)
    )

    evaluating_rmse = BranchSQLOperator(
        task_id='evaluating_rmse',
        sql='sql/FETCH_MIN_RMSE.sql',
        conn_id='postgres',
        follow_task_ids_if_true='accurate',
        follow_task_ids_if_false='inaccurate'
    )

    accurate = DummyOperator(
        task_id='accurate'
    )

    fetch_best_model = NotebookToKeepOperator(
        task_id='fetch_best_model',
        sql='sql/FETCH_BEST_MODEL.sql',
        postgres_conn_id='postgres'
    )

    publish_notebook = NotebookToGitOperator(
        task_id='publish_notebook',
        conn_id='git',
        nb_path='/tmp',
        nb_name='out-model-avocado-prediction-{{ task_instance.xcom_pull(key=None, task_ids="fetch_best_model") }}.ipynb'
    )

    innacurate = DummyOperator(
        task_id='inaccurate'
    )

    creating_accuray_table >> downloading_data >> waiting_for_data >> sanity_check >> training_model_tasks >> evaluating_rmse
    evaluating_rmse >> [accurate, innacurate]
    accurate >> fetch_best_model >> publish_notebook
