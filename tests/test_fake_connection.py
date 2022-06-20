from unittest import mock

from airflow.models.connection import Connection


def test_fake_connection():
    conn = Connection(
        conn_id='toloka_conn',
        conn_type='toloka',
        extra={
            'extra__toloka__token': 'fake_token',
            'extra__toloka__environment': 'SANDBOX',
        },
    )
    conn_uri = conn.get_uri()

    with mock.patch.dict('os.environ', AIRFLOW_CONN_TOLOKA_CONN=conn_uri):
        assert Connection.get_connection_from_secrets('toloka_conn').conn_type == 'toloka'
        assert Connection.get_connection_from_secrets('toloka_conn').extra_dejson['extra__toloka__token'] == 'fake_token'
        assert Connection.get_connection_from_secrets('toloka_conn').extra_dejson['extra__toloka__environment'] == 'SANDBOX'
