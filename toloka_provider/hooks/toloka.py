from typing import Optional

from toloka.client import TolokaClient

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class TolokaHook(BaseHook):
    """
    Interact with Toloka.

    Performs a connection to Toloka and retrieves client.

    :param toloka_conn_id: Your OAuth token for Toloka.
        You can learn more about how to get it [here](https://toloka.ai/docs/api/concepts/access.html#access__token).
    """

    default_conn_name: str = 'toloka_default'
    conn_type: str = 'toloka'
    conn_name_attr: str = 'toloka_conn_id'
    hook_name: str = 'Toloka'

    def __init__(self, toloka_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.toloka_conn_id = toloka_conn_id
        self.client: Optional[TolokaClient] = None
        self.get_conn()

    def get_conn(self) -> TolokaClient:
        """Function that initiates a new Toloka connection with token"""
        if not self.client:
            self.log.debug('Creating toloka client for conn_id: %s', self.toloka_conn_id)

            if not self.toloka_conn_id:
                raise AirflowException('Failed to create toloka client. no toloka_conn_id provided')

            conn = self.get_connection(self.toloka_conn_id)

            extra_options = {} if conn.extra is None else conn.extra_dejson
            extra_options.setdefault('env', 'PRODUCTION')

            try:
                self.client = TolokaClient(
                    token=conn.password,
                    environment=extra_options.get('env')
                )
            except ValueError as toloka_error:
                raise AirflowException(f'Failed to create toloka client, toloka error: {str(toloka_error)}')
            except Exception as e:
                raise AirflowException(f'Failed to create toloka client, error: {str(e)}')

        return self.client
