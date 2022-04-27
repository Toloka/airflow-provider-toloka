import os
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


@pytest.fixture(autouse=True, scope='session')
def reset_db():
    db.resetdb()
