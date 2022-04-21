import unittest
from unittest.mock import Mock, patch

from airflow.models import Connection
from airflow.utils import db

from toloka_provider.hooks.toloka import TolokaHook

toloka_client_mock = Mock(name='toloka_client')


class TestTolokaHook(unittest.TestCase):
    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='toloka_default',
                conn_type='toloka',
                password='password',
                extra='',
            )
        )

    @patch('toloka_provider.hooks.toloka.TolokaClient', autospec=True, return_value=toloka_client_mock)
    def test_toloka_client_connection(self, toloka_mock):
        toloka_hook = TolokaHook()

        assert toloka_hook.get_conn().name == toloka_mock.return_value.name
