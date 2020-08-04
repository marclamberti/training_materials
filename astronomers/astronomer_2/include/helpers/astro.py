import urllib.request

def download_dataset():
	url = 'https://raw.githubusercontent.com/marclamberti/training_materials/master/data/avocado.csv'
	urllib.request.urlretrieve(url, filename='/tmp/avocado.csv')

def read_rmse():
    accuracy = 0
    with open('/tmp/out-model-avocado-prediction-rmse.txt') as f:
        accuracy = float(f.readline())
    return 'accurate' if accuracy < 0.15 else 'inaccurate'