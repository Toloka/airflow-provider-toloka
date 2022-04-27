import pytest
from unittest import mock

from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from toloka.client import Training, structure
import toloka_provider.tasks.toloka as tlk_tasks

from ..time_config import DATA_INTERVAL_START, DATA_INTERVAL_END


@pytest.fixture
def training_map():
    return {
        'project_id': '10',
        'private_name': 'training_v12_231',
        'may_contain_adult_content': True,
        'mix_tasks_in_creation_order': True,
        'shuffle_tasks_in_task_suite': True,
        'training_tasks_in_task_suite_count': 3,
        'task_suites_required_to_pass': 5,
        'retry_training_after_days': 1,
        'inherited_instructions': True,
        'metadata': {'testKey': ['testValue']},
        'assignment_max_duration_seconds': 600,
        'public_instructions': 'text',
    }


@pytest.fixture
def training_map_with_readonly(training_map):
    return {
        **training_map,
        'id': '21',
        'owner': {'id': 'requester-1', 'myself': True, 'company_id': '1'},
        'created': '2015-12-16T12:55:01',
        'last_started': '2015-12-17T08:00:01',
        'last_stopped': '2015-12-18T08:00:01',
        'last_close_reason': 'MANUAL',
        'status': 'CLOSED',
    }


@pytest.fixture
def open_training_map_with_readonly(training_map_with_readonly):
    return {
        **training_map_with_readonly,
        'status': 'OPEN',
    }


@pytest.fixture
def dag_for_test_exam_creation(training_map, training_map_with_readonly):

    @task
    def prepare_training():
        return training_map

    @task
    def check_training(training):
        assert Training.from_json(training) == structure(training_map_with_readonly, Training)

    @dag(schedule_interval='@once', default_args={'start_date': DATA_INTERVAL_START})
    def dag_exam():
        training = prepare_training()
        created_training = tlk_tasks.create_exam_pool(obj=training, toloka_conn_id='toloka_conn')
        check_training(created_training)

    return dag_exam()


def test_create_training(requests_mock, dag_for_test_exam_creation, training_map, training_map_with_readonly, toloka_url):
    conn = Connection(
        conn_id='toloka_test',
        conn_type='toloka_test',
        password='fake_token',
        extra={
            'env': 'SANDBOX',
        },
    )
    conn_uri = conn.get_uri()

    def trainings(request, context):
        assert Training.from_json(request._request.body) == structure(training_map, Training)
        return training_map_with_readonly

    requests_mock.post(f'{toloka_url}/trainings', json=trainings, status_code=201)

    with mock.patch.dict('os.environ', AIRFLOW_CONN_TOLOKA_CONN=conn_uri):

        dagrun = dag_for_test_exam_creation.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=DATA_INTERVAL_START,
            data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )
        prepare_training = dagrun.get_task_instance(task_id='prepare_training')
        prepare_training.task = dag_for_test_exam_creation.get_task(task_id='prepare_training')
        prepare_training.run(ignore_ti_state=True)

        create_training = dagrun.get_task_instance(task_id='create_exam_pool')
        create_training.task = dag_for_test_exam_creation.get_task(task_id='create_exam_pool')
        create_training.run(ignore_ti_state=True)

        check_training = dagrun.get_task_instance(task_id='check_training')
        check_training.task = dag_for_test_exam_creation.get_task(task_id='check_training')
        check_training.run(ignore_ti_state=True)


@pytest.fixture
def dag_for_test_open_training(training_map, open_training_map_with_readonly):

    @task
    def prepare_training():
        return training_map

    @task
    def check_training(training):
        assert Training.from_json(training) == structure(open_training_map_with_readonly, Training)

    @dag(schedule_interval='@once', default_args={'start_date': DATA_INTERVAL_START})
    def dag_open_training():
        training = prepare_training()
        created_training = tlk_tasks.create_exam_pool(obj=training, toloka_conn_id='toloka_conn')
        opened_training = tlk_tasks.open_exam_pool(obj=created_training, toloka_conn_id='toloka_conn')
        check_training(opened_training)

    return dag_open_training()


def test_open_training(requests_mock, dag_for_test_open_training, training_map, training_map_with_readonly,
                   open_training_map_with_readonly, toloka_url):
    conn = Connection(
        conn_id='toloka_test',
        conn_type='toloka_test',
        password='fake_token',
        extra={
            'env': 'SANDBOX',
        },
    )
    conn_uri = conn.get_uri()

    def trainings(request, context):
        assert Training.from_json(request._request.body) == structure(training_map, Training)
        return training_map_with_readonly

    def get_training(request, context):
        return open_training_map_with_readonly

    requests_mock.post(f'{toloka_url}/trainings', json=trainings, status_code=201)
    requests_mock.post(f'{toloka_url}/trainings/21/open', status_code=204)
    requests_mock.get(f'{toloka_url}/trainings/21', json=get_training)

    with mock.patch.dict('os.environ', AIRFLOW_CONN_TOLOKA_CONN=conn_uri):

        dagrun = dag_for_test_open_training.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=DATA_INTERVAL_START,
            data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )
        prepare_training = dagrun.get_task_instance(task_id='prepare_training')
        prepare_training.task = dag_for_test_open_training.get_task(task_id='prepare_training')
        prepare_training.run(ignore_ti_state=True)

        create_training = dagrun.get_task_instance(task_id='create_exam_pool')
        create_training.task = dag_for_test_open_training.get_task(task_id='create_exam_pool')
        create_training.run(ignore_ti_state=True)

        open_training = dagrun.get_task_instance(task_id='open_exam_pool')
        open_training.task = dag_for_test_open_training.get_task(task_id='open_exam_pool')
        open_training.run(ignore_ti_state=True)

        check_training = dagrun.get_task_instance(task_id='check_training')
        check_training.task = dag_for_test_open_training.get_task(task_id='check_training')
        check_training.run(ignore_ti_state=True)
