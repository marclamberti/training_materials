import urllib.request
from airflow.models import Variable
import os

def download_dataset(ti):
    dataset = Variable.get('avocado_dag_dataset_settings', deserialize_json=True)
    output = f"{dataset['filepath']}{dataset['filename']}"
    urllib.request.urlretrieve(dataset['url'], filename=output)
    return os.path.getsize(output)


def check_dataset(ti):
    filesize = int(ti.xcom_pull(task_ids=['downloading_data'])[0])
    if filesize <= 0:
        raise ValueError('Dataset is empty')

def read_rmse():
    accuracy = 0
    with open('/tmp/out-model-avocado-prediction-rmse.txt') as f:
        accuracy = float(f.readline())
    return 'accurate' if accuracy < 0.15 else 'inaccurate'