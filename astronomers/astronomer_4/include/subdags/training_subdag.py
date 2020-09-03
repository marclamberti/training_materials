from airflow.models import DAG, Variable
from airflow.operators.papermill_operator import PapermillOperator

def subdag_factory(parent_dag_name, child_dag_name, default_args):
    with DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=default_args
    ) as dag_subdag:

        model_settings = Variable.get('avocado_dag_model_settings', deserialize_json=True)
        training_model_tasks = []

        for feature in model_settings['max_features']:
            for estimator in model_settings['n_estimators']:
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

        return dag_subdag