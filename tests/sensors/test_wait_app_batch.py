import copy

import pytest
from unittest import mock

from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from toloka.client import structure, AppBatch
import toloka_provider.sensors.toloka as tlk_sensors

from ..time_config import DATA_INTERVAL_START, DATA_INTERVAL_END


@pytest.fixture
def completed_app_batch_map():
    return {
        'validation_errors': {},
        'id': '123',
        'app_project_id': '321',
        'name': 'Test batch',
        'status': 'COMPLETED',
        'items_count': 2000,
        'item_price': 0.5,
        'cost': 0.0,
        'cost_of_processed': 0.0,
        'created_at': '2022-06-21T20:54:13.996000',
        'started_at': '2022-06-21T20:54:26.678000',
        'finished_at': '2022-06-28T08:40:01.359000',
        'read_only': False,
        'confidence_avg': 0.89,
        'items_processed_count': 2000,
        'eta': '2022-06-28T08:40:01.327000',
        'items_per_state': {
            'NEW': 0,
            'PROCESSING': 0,
            'COMPLETED': 2000,
            'ERROR': 0,
            'CANCELLED': 0,
            'NO_MONEY': 0,
            'ARCHIVE': 0,
            'STOPPED': 0,
        },
    }


@pytest.fixture
def new_app_batch_map(completed_app_batch_map):
    app_batch_map = copy.deepcopy(completed_app_batch_map)
    app_batch_map['status'] = 'NEW'
    app_batch_map['items_processed_count'] = 0
    app_batch_map['items_per_state']['COMPLETED'] = 0
    app_batch_map['items_per_state']['NEW'] = 2000
    return app_batch_map


@pytest.fixture
def processing_app_batch_map(completed_app_batch_map):
    app_batch_map = copy.deepcopy(completed_app_batch_map)
    app_batch_map['status'] = 'PROCESSING'
    app_batch_map['items_processed_count'] = 1000
    app_batch_map['items_per_state']['COMPLETED'] = 1000
    app_batch_map['items_per_state']['NEW'] = 1000
    return app_batch_map


@pytest.fixture
def dag_for_test_wait_app_batch(new_app_batch_map, completed_app_batch_map):
    @task
    def prepare_app_batch():
        return new_app_batch_map

    @task
    def check_app_batch(batch):
        assert AppBatch.from_json(batch) == structure(completed_app_batch_map, AppBatch)

    @dag(schedule_interval='@once', default_args={'start_date': DATA_INTERVAL_START})
    def dag_wait_app_batch():
        app_batch = prepare_app_batch()
        _waiting = tlk_sensors.WaitAppBatchSensor(
            task_id='wait_app_batch', app_project='321', batch=app_batch,
            toloka_conn_id='toloka_conn', success_on_statuses=['COMPLETED'],
            poke_interval=0.1,
        )
        _check = check_app_batch(batch=app_batch)
        _waiting >> _check

    return dag_wait_app_batch()


def test_wait_closed_app_batch(
    requests_mock,
    dag_for_test_wait_app_batch,
    new_app_batch_map,
    processing_app_batch_map,
    completed_app_batch_map,
    toloka_client_prod,
):
    conn = Connection(
        conn_id='toloka_conn',
        conn_type='toloka',
        extra={
            'extra__toloka__token': 'fake_token',
            'extra__toloka__environment': 'PRODUCTION',
        },
    )
    conn_uri = conn.get_uri()
    requests_count = 0

    def app_batch(request, context):
        nonlocal requests_count
        requests_count += 1
        if requests_count == 1:
            return new_app_batch_map
        elif requests_count == 2:
            return processing_app_batch_map
        else:
            return completed_app_batch_map

    requests_mock.get(f'{toloka_client_prod.url}/api/app/v0/app-projects/321/batches/123', json=app_batch)

    with mock.patch.dict('os.environ', AIRFLOW_CONN_TOLOKA_CONN=conn_uri):

        dagrun = dag_for_test_wait_app_batch.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=DATA_INTERVAL_START,
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )

        prepare_app_batch = dagrun.get_task_instance(task_id='prepare_app_batch')
        prepare_app_batch.task = dag_for_test_wait_app_batch.get_task(
            task_id='prepare_app_batch'
        )
        prepare_app_batch.run(ignore_ti_state=True)

        wait_app_batch = dagrun.get_task_instance(task_id='wait_app_batch')
        wait_app_batch.task = dag_for_test_wait_app_batch.get_task(task_id='wait_app_batch')
        wait_app_batch.run(ignore_ti_state=True)
