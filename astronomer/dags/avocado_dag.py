from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.papermill_operator import PapermillOperator

from include.helpers.astro import download_dataset, read_rmse
from datetime import datetime, timedelta

default_args = {
    'retries': 3,
    'email_on_retry': False,
    'email_on_failure': False,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False
}

with DAG('avocado_dag', default_args=default_args, 
    description='Forecasting avocado prices', 
    schedule_interval='*/10 * * * *', 
    start_date=datetime(2020, 1, 1), catchup=False) as dag:

    downloading_data = PythonOperator(
        task_id='downloading_data',
        python_callable=download_dataset
    )

    waiting_for_data = FileSensor(
        task_id='waiting_for_data',
        fs_conn_id='fs_default',
        filepath='avocado.csv',
        poke_interval=15
    )

    training_model = PapermillOperator(
        task_id='training_model',
        input_nb='/usr/local/airflow/include/notebooks/avocado_prediction.ipynb',
        output_nb='/tmp/out-model-avocado-prediction.ipynb',
        parameters={
            'filepath': '/tmp/avocado.csv',
            'n_estimators': 100,
            'max_features': 'auto',
            'output_rmse': '/tmp/out-model-avocado-prediction-rmse.txt'
        }
    )

    evualating_rmse = BranchPythonOperator(
        task_id="evualating_rmse",
        python_callable=read_rmse
    )

    accurate = DummyOperator(
        task_id='accurate'
    )

    inaccurate = DummyOperator(
        task_id='inaccurate'
    )

    downloading_data >> waiting_for_data >> training_model >> evualating_rmse
    evualating_rmse >> [accurate,inaccurate]



	