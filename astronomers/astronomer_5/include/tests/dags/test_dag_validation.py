import pytest
from airflow.models import DagBag

@pytest.fixture(scope="class")
def dagbag():
    return DagBag()

class TestDagValidation:

    EXPECTED_NUMBER_OF_DAGS = 4
    REQUIRED_EMAIL = 'support@honeywell.com'

    def test_import_dags(self, dagbag):
        assert len(dagbag.import_errors) == 0, "DAG Failures"

    def test_number_of_dags(self, dagbag):
        stats = dagbag.dagbag_stats
        dag_num = sum([o.dag_num for o in stats])
        assert dag_num == self.EXPECTED_NUMBER_OF_DAGS, "Wrong number of dags"

    def test_default_args_email(self, dagbag):
        for dag_id, dag in dagbag.dags.items():
            emails = dag.default_args.get('email', [])
            assert self.REQUIRED_EMAIL in emails, "The email is wrong"

    
