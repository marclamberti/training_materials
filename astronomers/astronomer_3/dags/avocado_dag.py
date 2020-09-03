from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.papermill_operator import PapermillOperator
from include.helpers.astro import download_dataset, read_rmse, check_dataset

from datetime import datetime, timedelta

default_args = {
    'retries': 3,
    'email_on_retry': False,
    'email_on_failure': False,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False
}

with DAG('avocado_dag', 
    default_args=default_args, 
    description='Forecasting avocado prices', 
    schedule_interval='*/10 * * * *', 
    start_date=datetime(2020, 1, 1), 
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

    n_estimators = [100, 150]
    max_features = ['auto', 'sqrt']
    training_model_tasks = []

    for feature in max_features:
        for estimator in n_estimators:
            ml_id = feature + '_' + str(estimator)
            training_model_tasks.append(PapermillOperator(
                task_id='training_model_{0}'.format(ml_id),
                input_nb='/usr/local/airflow/include/notebooks/avocado_prediction.ipynb',
                output_nb='/tmp/out-model-avocado-prediction-{0}.ipynb'.format(ml_id),
                pool='training_pool',
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

    innacurate = DummyOperator(
        task_id='inaccurate'
    )

    creating_accuray_table >> downloading_data >> waiting_for_data >> sanity_check >> training_model_tasks >> evaluating_rmse
    evaluating_rmse >> [accurate, innacurate]
