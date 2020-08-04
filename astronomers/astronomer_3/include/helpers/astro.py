import urllib.request
import os
from airflow.models import Variable

def download_dataset():
    dataset = Variable.get("avocado_dag_dataset_settings", deserialize_json=True)
    output = dataset['filepath'] + dataset['filename']
    urllib.request.urlretrieve(dataset['url'], filename=output)
    return os.path.getsize(output)

def check_dataset(**kwargs):
    ti = kwargs['ti']
    filesize = ti.xcom_pull(key=None, task_ids='downloading_data')
    if filesize <= 0:
        raise ValueError('Dataset is empty')