import pytest
from airflow.models import DagBag

@pytest.fixture(scope="class")
def dag():
    return DagBag().get_dag('avocado_dag')

class TestAvocadoDagDefinition:

    def test_nb_task(self, dag):
        nb_tasks = len(dag.tasks)
        assert nb_tasks == 10
