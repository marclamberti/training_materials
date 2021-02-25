from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from include.helpers.astro import download_dataset,read_rmse,check_dataset
from airflow.sensors.filesystem import FileSensor
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.sql import BranchSQLOperator

from datetime import datetime, timedelta

default_args = {
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG('avocado_dag', default_args=default_args, 
    description='Forecasting avocado prices', 
    schedule_interval='*/10 * * * *', start_date=datetime(2020, 1, 1), catchup=False) as dag:

    creating_table = PostgresOperator(
        task_id='creating_table',
        sql='sql/CREATE_TABLE_ACCURACIES.sql',
        postgres_conn_id='postgres'
    )

    downloading_data = PythonOperator(
        task_id='downloading_data',
        python_callable=download_dataset
    )

    sanity_check = PythonOperator(
        task_id="sanity_check",
        python_callable=check_dataset
    )

    waiting_for_data = FileSensor(
        task_id='waiting_for_data',
        fs_conn_id='fs_default',
        filepath='avocado.csv',
        poke_interval=15
    )

    n_estimators = [100, 150]
    max_features = ['auto','sqrt']

    training_model_tasks = []
    for feature in max_features:
        for estimator in n_estimators:
            ml_id = f"{feature}_{estimator}"
            training_model_tasks.append(PapermillOperator(
                task_id=f'training_model_{ml_id}',
                input_nb='/usr/local/airflow/include/notebooks/avocado_prediction.ipynb',
                output_nb=f'/tmp/out-model-avocado-prediction-{ml_id}.ipynb',
                parameters={
                    'filepath': '/tmp/avocado.csv',
                    'n_estimators': estimator,
                    'max_features': feature,
                    'ml_id': ml_id
                }
            ))

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

    inaccurate = DummyOperator(
        task_id='inaccurate'
    )

    creating_table >> downloading_data >> sanity_check >> waiting_for_data >> training_model_tasks >> evaluating_rmse
    evaluating_rmse >> [ accurate, inaccurate ]


	