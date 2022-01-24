from toloka.client import TolokaClient
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from typing import Optional


class TolokaHook(BaseHook):
    default_conn_name = 'toloka_default'
    conn_type = "toloka"
    conn_name_attr = "toloka_conn_id"
    hook_name = "TOLOKA"

    def __init__(self, toloka_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.toloka_conn_id = toloka_conn_id
        self.client: Optional[TolokaClient] = None
        self.get_conn()

    def get_conn(self) -> TolokaClient:
        if not self.client:
            self.log.debug('Creating toloka client for conn_id: %s', self.toloka_conn_id)

            extra_options = {}
            if not self.toloka_conn_id:
                raise AirflowException('Failed to create toloka client. no toloka_conn_id provided')

            conn = self.get_connection(self.toloka_conn_id)
            if conn.extra is not None:
                extra_options = conn.extra_dejson

                if 'env' in extra_options and extra_options['env'].upper() == 'SANDBOX':
                    extra_options['env'] = 'SANDBOX'
                else:
                    extra_options['env'] = 'PRODUCTION'

            try:
                self.client = TolokaClient(
                    token=conn.password,
                    environment=extra_options['env']
                )
            except ValueError as toloka_error:
                raise AirflowException(f'Failed to create toloka client, toloka error: {str(toloka_error)}')
            except Exception as e:
                raise AirflowException(f'Failed to create toloka client, error: {str(e)}')

        return self.client
