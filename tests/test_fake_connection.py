from unittest import mock

from airflow.models.connection import Connection


def test_fake_connection():
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
        assert Connection.get_connection_from_secrets('toloka_conn').conn_type == 'toloka_test'
        assert Connection.get_connection_from_secrets('toloka_conn').extra_dejson['env'] == 'SANDBOX'
