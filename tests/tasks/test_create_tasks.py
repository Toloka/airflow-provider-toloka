import pytest
import logging
from unittest import mock
from urllib.parse import urlparse, parse_qs

from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from toloka.client.batch_create_results import TaskBatchCreateResult
import toloka_provider.tasks.toloka as tlk_tasks

from ..time_config import DATA_INTERVAL_START, DATA_INTERVAL_END


@pytest.fixture
def tasks_map():
    return [
        {'pool_id': '21', 'input_values': {'image': 'http://images.com/1.png'}},
        {'pool_id': '22', 'input_values': {'image': 'http://images.com/2.png'}},
        {'pool_id': '22', 'input_values': {'imagis': 'http://images.com/3.png'}},
    ]


@pytest.fixture
def sync_additional_params():
    return {
        'operation_id': '281073ea-ab34-416e-a028-47421ff1b166',
        'skip_invalid_items': True,
        'allow_defaults': True,
        'open_pool': True,
        'async_mode': False,
    }


@pytest.fixture
def task_create_result_map():
    return {
        'items': {
            '0': {
                'input_values': {'image': 'http://images.com/1.png'},
                'id': '00014495f0--60213f7c25a8b84e2ffb7a2c',
                'infinite_overlap': False,
                'overlap': 1,
                'pool_id': '21',
                'remaining_overlap': 1,
            },
            '1': {
                'input_values': {'image': 'http://images.com/2.png'},
                'id': '00014495f0--60213f7c25a8b84e2ffb7a3b',
                'infinite_overlap': False,
                'overlap': 1,
                'pool_id': '22',
                'remaining_overlap': 1,
            },
        },
        'validation_errors':
        {
            '2': {
                'input_values.image': {
                    'code': 'VALUE_REQUIRED',
                    'message': 'Value must be present and not equal to null',
                },
                'input_values.imagis': {
                    'code': 'VALUE_NOT_ALLOWED',
                    'message': 'Unknown field name',
                },
            },
        },
    }


@pytest.fixture
def dag_for_test_sync_tasks_creation(tasks_map, sync_additional_params):

    @task
    def prepare_tasks():
        return tasks_map

    @dag(schedule_interval='@once', default_args={'start_date': DATA_INTERVAL_START})
    def dag_tasks():
        tasks = prepare_tasks()
        tlk_tasks.create_tasks(tasks=tasks, toloka_conn_id='toloka_conn', additional_args=sync_additional_params)

    return dag_tasks()


def test_create_tasks(requests_mock, dag_for_test_sync_tasks_creation, tasks_map, task_create_result_map, toloka_url, caplog):
    conn = Connection(
        conn_id='toloka_test',
        conn_type='toloka_test',
        password='fake_token',
        extra={
            'env': 'SANDBOX',
        },
    )
    conn_uri = conn.get_uri()

    def tasks(request, context):
        assert {
            'allow_defaults': ['true'],
            'async_mode': ['false'],
            'operation_id': ['281073ea-ab34-416e-a028-47421ff1b166'],
            'skip_invalid_items': ['true'],
            'open_pool': ['true'],
        } == parse_qs(urlparse(request.url).query)
        return task_create_result_map

    requests_mock.post(f'{toloka_url}/tasks', json=tasks, status_code=201)

    with mock.patch.dict('os.environ', AIRFLOW_CONN_TOLOKA_CONN=conn_uri):

        dagrun = dag_for_test_sync_tasks_creation.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=DATA_INTERVAL_START,
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )
        prepare_tasks = dagrun.get_task_instance(task_id='prepare_tasks')
        prepare_tasks.task = dag_for_test_sync_tasks_creation.get_task(task_id='prepare_tasks')
        prepare_tasks.run(ignore_ti_state=True)

        with caplog.at_level(logging.INFO):
            caplog.clear()
            create_tasks = dagrun.get_task_instance(task_id='create_tasks')
            create_tasks.task = dag_for_test_sync_tasks_creation.get_task(task_id='create_tasks')
            create_tasks.run(ignore_ti_state=True)

            result = TaskBatchCreateResult.structure(task_create_result_map)
            assert caplog.record_tuples.count(('toloka_provider.tasks.toloka', logging.INFO, f'Tasks: {result} created')) == 1
