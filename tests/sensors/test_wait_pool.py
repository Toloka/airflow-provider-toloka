import pytest
from unittest import mock

from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from toloka.client import Pool, structure
import toloka_provider.sensors.toloka as tlk_sensors

from ..time_config import DATA_INTERVAL_START, DATA_INTERVAL_END


@pytest.fixture
def pool_map():
    return {
        'type': 'REGULAR',
        'project_id': '10',
        'private_name': 'pool_v12_231',
        'public_description': '42',
        'may_contain_adult_content': True,
        'will_expire': '2016-03-23T12:59:00',
        'auto_close_after_complete_delay_seconds': 600,
        'reward_per_assignment': 0.03,
        'dynamic_pricing_config': {
            'type': 'SKILL',
            'skill_id': '123123',
            'intervals': [
                {'from': 50, 'to': 79, 'reward_per_assignment': 0.05},
                {'from': 80, 'reward_per_assignment': 0.1},
            ]
        },
        'dynamic_overlap_config': {
            'type': 'BASIC',
            'max_overlap': 5,
            'min_confidence': 0.95,
            'answer_weight_skill_id': '42',
            'fields': [{'name': 'out1'}],
        },
        'metadata': {'testKey': ['testValue']},
        'assignment_max_duration_seconds': 600,
        'auto_accept_solutions': True,
        'priority': 10,
        'defaults': {
            'default_overlap_for_new_task_suites': 3,
            'default_overlap_for_new_tasks': 2,
        },
        'mixer_config': {
            'real_tasks_count': 10,
            'golden_tasks_count': 2,
            'training_tasks_count': 1,
            'min_training_tasks_count': 0,
            'min_golden_tasks_count': 1,
            'force_last_assignment': False,
            'force_last_assignment_delay_seconds': 10,
            'mix_tasks_in_creation_order': False,
            'shuffle_tasks_in_task_suite': True,
            'golden_task_distribution_function': {
                'scope': 'POOL',
                'distribution': 'UNIFORM',
                'window_days': 5,
                'intervals': [
                    {'to': 50, 'frequency': 5},
                    {'from': 100, 'frequency': 50},
                ],
            },
        },
        'assignments_issuing_config': {
            'issue_task_suites_in_creation_order': True,
        },
        'filter': {
            'and': [
                {
                    'category': 'profile',
                    'key': 'adult_allowed',
                    'operator': 'EQ',
                    'value': True,
                },
                {
                    'or': [
                        {
                            'category': 'skill',
                            'key': '20',
                            'operator': 'GTE',
                            'value': 60,
                        },
                        {
                            'category': 'skill',
                            'key': '22',
                            'operator': 'GT',
                            'value': 95,
                        },
                    ],
                },
            ],
        },
        'quality_control': {
            'captcha_frequency': 'LOW',
            'checkpoints_config': {
                'real_settings': {
                    'target_overlap': 5,
                    'task_distribution_function': {
                        'scope': 'PROJECT',
                        'distribution': 'UNIFORM',
                        'window_days': 7,
                        'intervals': [
                            {'to': 100, 'frequency': 5},
                            {'from': 101, 'frequency': 50},
                        ],
                    },
                },
            },
            'configs': [
                {
                    'collector_config': {
                        'type': 'CAPTCHA',
                        'parameters': {'history_size': 5},
                    },
                    'rules': [
                        {
                            'conditions': [
                                {
                                    'key': 'stored_results_count',
                                    'operator': 'EQ',
                                    'value': 5,
                                },
                                {
                                    'key': 'success_rate',
                                    'operator': 'LTE',
                                    'value': 60.0,
                                },
                            ],
                            'action': {
                                'type': 'RESTRICTION',
                                'parameters': {
                                    'scope': 'POOL',
                                    'duration_days': 10,
                                    'private_comment': 'ban in pool',
                                },
                            },
                        },
                    ],
                },
            ],
        },
    }


@pytest.fixture
def pool_map_with_readonly(pool_map):
    return {
        **pool_map,
        'id': '21',
        'owner': {'id': 'requester-1', 'myself': True, 'company_id': '1'},
        'type': 'REGULAR',
        'created': '2015-12-16T12:55:01',
        'last_started': '2015-12-17T08:00:01',
        'last_stopped': '2015-12-18T08:00:01',
        'last_close_reason': 'MANUAL',
        'status': 'CLOSED',
    }


@pytest.fixture
def success_answer_map():
    return {
        'id': '8ee7e276-53bb-4220-823e-05f578392915',
        'type': 'ANALYTICS',
        'status': 'SUCCESS',
        'submitted': '2021-08-24T06:30:08.394',
        'started': '2021-08-24T06:30:08.51',  # 2 ms digits
        'finished': '2021-08-24T06:30:11.99999',  # 5 ms digits
        'progress': 100,
        'details':
            {
                'value':
                    [
                        {
                            'result': {
                                'value': 100,
                            },
                            'request':
                                {
                                    'name': 'submitted_assignments_count',
                                    'subject': 'POOL',
                                    'subject_id': '26807107'
                                },
                            'finished': '2021-08-24T06:30:08.923461'
                        },
                    ]
            }
    }


@pytest.fixture
def dag_for_test_wait_closed_pool(pool_map_with_readonly):

    @task
    def prepare_pool():
        return pool_map_with_readonly

    @task
    def check_pool(pool):
        assert Pool.from_json(pool) == structure(pool_map_with_readonly, Pool)

    @dag(schedule_interval='@once', default_args={'start_date': DATA_INTERVAL_START})
    def dag_wait_pool():
        pool = prepare_pool()
        _waiting = tlk_sensors.wait_pool(pool, toloka_conn_id='toloka_conn')
        _check = check_pool(pool)
        _waiting >> _check

    return dag_wait_pool()


def test_wait_closed_pool(requests_mock, dag_for_test_wait_closed_pool, pool_map_with_readonly, success_answer_map, toloka_url, toloka_api_url):
    conn = Connection(
        conn_id='toloka_test',
        conn_type='toloka_test',
        password='fake_token',
        extra={
            'env': 'SANDBOX',
        },
    )
    conn_uri = conn.get_uri()

    with mock.patch.dict('os.environ', AIRFLOW_CONN_TOLOKA_CONN=conn_uri):
        def task_suites(request, context):
            return success_answer_map

        def pool(request, context):
            return pool_map_with_readonly

        requests_mock.post(f'{toloka_api_url}/staging/analytics-2', json=task_suites)
        requests_mock.get(f'{toloka_url}/pools/21', json=pool)

        dagrun = dag_for_test_wait_closed_pool.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=DATA_INTERVAL_START,
            data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )

        prepare_pool = dagrun.get_task_instance(task_id='prepare_pool')
        prepare_pool.task = dag_for_test_wait_closed_pool.get_task(task_id='prepare_pool')
        prepare_pool.run(ignore_ti_state=True)

        wait_pool = dagrun.get_task_instance(task_id='wait_pool')
        wait_pool.task = dag_for_test_wait_closed_pool.get_task(task_id='wait_pool')
        wait_pool.run(ignore_ti_state=True)
