import pytest
from unittest import mock
from urllib.parse import urlparse, parse_qs
from decimal import Decimal
import simplejson
import io
from datetime import datetime

import pandas as pd

from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from toloka.client import Assignment, structure
import toloka_provider.tasks.toloka as tlk_tasks

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
            ],
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
def assignment_map():
    return {
        'id': 'assignment-i1d',
        'task_suite_id': 'task-suite-i1d',
        'pool_id': '21',
        'user_id': 'user-i1d',
        'status': 'ACCEPTED',
        'reward': Decimal('0.05'),
        'mixed': True,
        'automerged': True,
        'created': '2015-12-15T14:52:00',
        'submitted': '2015-12-15T15:10:00',
        'accepted': '2015-12-15T20:00:00',
        'tasks': [{'pool_id': '21', 'input_values': {'image': 'http://images.com/1.png'}, 'origin_task_id': '42'}],
        'first_declined_solution_attempt': [{'output_values': {'color': 'black', 'comment': 'So белый'}}],
        'solutions': [{'output_values': {'color': 'white', 'comment': 'So белый'}}],
    }


@pytest.fixture
def prepared_assignments(assignment_map):
    assignments = [{**assignment_map, 'id': f'assignment-i{i}d'} for i in range(50)]
    assignments.sort(key=lambda assignment: assignment['id'])
    return assignments


@pytest.fixture
def dag_for_test_get_assignments(pool_map, prepared_assignments):

    @task
    def prepare_pool():
        return pool_map

    @task
    def check_assignments(assignments):
        assert [Assignment.from_json(assignment) for assignment in assignments] == \
               [structure(prep, Assignment) for prep in prepared_assignments]

    @dag(schedule_interval='@once', default_args={'start_date': DATA_INTERVAL_START})
    def dag_assignments():
        pool = prepare_pool()
        assignments = tlk_tasks.get_assignments(
            pool=pool,
            status=Assignment.Status.ACCEPTED,
            toloka_conn_id='toloka_conn',
            additional_args={
                'user_id': 'user-i1d',
                'created_gte': datetime(2015, 12, 1),
                'created_lt': datetime(2016, 6, 1),
            },
        )
        check_assignments(assignments)

    return dag_assignments()


def test_get_assignments(requests_mock, dag_for_test_get_assignments, prepared_assignments, toloka_url):
    conn = Connection(
        conn_id='toloka_test',
        conn_type='toloka_test',
        password='fake_token',
        extra={
            'env': 'SANDBOX',
        },
    )
    conn_uri = conn.get_uri()

    def assignments(request, context):
        params = parse_qs(urlparse(request.url).query)
        id_gt = params.pop('id_gt')[0] if 'id_gt' in params else None
        assert params == {
            'status': ['ACCEPTED'],
            'pool_id': ['21'],
            'user_id': ['user-i1d'],
            'created_gte': ['2015-12-01T00:00:00'],
            'created_lt': ['2016-06-01T00:00:00'],
            'sort': ['id'],
        }

        items = [assignment for assignment in prepared_assignments if id_gt is None or assignment['id'] > id_gt][:3]
        result = simplejson.dumps({
            'items': items,
            'has_more': items[-1]['id'] != prepared_assignments[-1]['id'],
        })
        return result

    requests_mock.get(f'{toloka_url}/assignments', text=assignments)

    with mock.patch.dict('os.environ', AIRFLOW_CONN_TOLOKA_CONN=conn_uri):

        dagrun = dag_for_test_get_assignments.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=DATA_INTERVAL_START,
            data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )

        prepare_pool = dagrun.get_task_instance(task_id='prepare_pool')
        prepare_pool.task = dag_for_test_get_assignments.get_task(task_id='prepare_pool')
        prepare_pool.run(ignore_ti_state=True)

        get_assignments = dagrun.get_task_instance(task_id='get_assignments')
        get_assignments.task = dag_for_test_get_assignments.get_task(task_id='get_assignments')
        get_assignments.run(ignore_ti_state=True)

        check_assignments = dagrun.get_task_instance(task_id='check_assignments')
        check_assignments.task = dag_for_test_get_assignments.get_task(task_id='check_assignments')
        check_assignments.run(ignore_ti_state=True)
