import pytest
from unittest import mock

from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from toloka.client import Project, structure
import toloka_provider.tasks.toloka as tlk_ops

from ..time_config import DATA_INTERVAL_START, DATA_INTERVAL_END


@pytest.fixture
def raw_project_map():
    return {
        'public_name': 'Map Task',
        'public_description': 'Simple map task',
        'public_instructions': 'Check if company exists',
        'task_spec': {
            'input_spec': {
                'point': {
                    'type': 'coordinates',
                    'required': True,
                },
                'company': {
                    'type': 'string',
                    'required': True,
                },
            },
            'output_spec': {
                'exists': {
                    'type': 'boolean',
                    'required': True,
                },
            },
            'view_spec': {
                'markup': '<dummy/>',
                'type': 'classic',
            }
        },
        'assignments_issuing_type': 'MAP_SELECTOR',
        'assignments_issuing_view_config': {
            'title_template': 'Company: {{inputParams[\'company\']}}',
            'description_template': 'Check if company {{inputParams[\'company\']}} exists',
        },
        'metadata': {'projectMetadataKey': ['projectMetadataValue']},
        'quality_control': {
            'config': [
                {
                    'collector_config': {
                        'type': 'ANSWER_COUNT',
                        'uuid': 'daab2575-374a-4006-b285-9760db09795c',
                    },
                    'rules': [
                        {
                            'action': {
                                'type': 'RESTRICTION',
                                'parameters': {
                                    'scope': 'POOL'
                                }
                            },
                            'conditions': [
                                {
                                    'key': 'assignments_accepted_count',
                                    'value': 42000,
                                    'operator': 'GTE',
                                }
                            ],
                        },
                    ],
                },
            ],
        },
        'assignments_automerge_enabled': True,
        'status': 'ACTIVE',
        'created': '2015-12-09T12:10:00',
    }


@pytest.fixture
def simple_project_map(raw_project_map):
    return {
        'id': 10,
        **raw_project_map,
    }


@pytest.fixture
def dag_for_test_project_creation(raw_project_map, simple_project_map):

    @task
    def prepare_project():
        return raw_project_map

    @task
    def check_project(project):
        assert Project.from_json(project) == structure(simple_project_map, Project)

    @dag(schedule_interval='@once', default_args={'start_date': DATA_INTERVAL_START})
    def dag_project():
        project = prepare_project()
        created_project = tlk_ops.create_project(obj=project, toloka_conn_id='toloka_conn')
        check_project(created_project)

    return dag_project()


def test_create_default_project(requests_mock, dag_for_test_project_creation, raw_project_map, simple_project_map, toloka_url):
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
        def project(request, context):
            assert Project.from_json(request._request.body) == structure(raw_project_map, Project)
            return simple_project_map

        requests_mock.post(f'{toloka_url}/projects', json=project, status_code=201)

        dagrun = dag_for_test_project_creation.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=DATA_INTERVAL_START,
            data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
            start_date=DATA_INTERVAL_END,
            run_type=DagRunType.MANUAL,
        )
        prepare_project = dagrun.get_task_instance(task_id='prepare_project')
        prepare_project.task = dag_for_test_project_creation.get_task(task_id='prepare_project')
        prepare_project.run(ignore_ti_state=True)

        create_project = dagrun.get_task_instance(task_id='create_project')
        create_project.task = dag_for_test_project_creation.get_task(task_id='create_project')
        create_project.run(ignore_ti_state=True)

        check_project = dagrun.get_task_instance(task_id='check_project')
        check_project.task = dag_for_test_project_creation.get_task(task_id='check_project')
        check_project.run(ignore_ti_state=True)
