import os
import shutil
import pytest

from airflow.utils import db
from toloka.client import TolokaClient


@pytest.fixture
def toloka_client() -> TolokaClient:
    return TolokaClient('fake_token', 'SANDBOX')


@pytest.fixture
def toloka_client_prod() -> TolokaClient:
    return TolokaClient('fake_token', 'PRODUCTION')


@pytest.fixture
def toloka_api_url(toloka_client) -> str:
    return f'{toloka_client.url}/api'


@pytest.fixture
def toloka_url(toloka_api_url) -> str:
    return f'{toloka_api_url}/v1'


os.environ['AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS'] = 'False'
os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'False'
os.environ['AIRFLOW__CORE__UNIT_TEST_MODE'] = 'True'
os.environ['AIRFLOW_HOME'] = os.path.dirname(os.path.dirname(__file__))


@pytest.fixture(autouse=True, scope='session')
def reset_db():
    db.resetdb()
