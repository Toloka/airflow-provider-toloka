import unittest
from unittest.mock import Mock, patch

from airflow.models import Connection
from airflow.utils import db

from toloka_provider.hooks.toloka import TolokaHook

toloka_client_mock = Mock(name="toloka_client")


class TestTolokaHook(unittest.TestCase):
    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='toloka_default',
                conn_type='toloka',
                host='https://localhost/toloka/',
                port=443,
                extra='{"verify": "False", "project": "AIRFLOW"}',
            )
        )

    @patch("toloka_provider.hooks.toloka.TolokaClient", autospec=True, return_value=toloka_client_mock)
    def test_toloka_client_connection(self, toloka_mock):
        toloka_hook = TolokaHook()

        assert toloka_mock.called
        assert isinstance(toloka_hook.client, Mock)
        assert toloka_hook.client.name == toloka_mock.return_value.name
