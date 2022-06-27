"""
Module contains TolokaHook. It gets token from Airflow Connections and creates TolokaClient.
(see https://github.com/Toloka/toloka-kit for more information about functionality of TolokaClient)
"""
from typing import Optional, Dict, Any

from toloka.client import TolokaClient

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class TolokaHook(BaseHook):
    """
    Hook to interact with Toloka.

    Performs a connection to Toloka and retrieves client.

    :param toloka_conn_id: Airflow Connection with OAuth token for Toloka.
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

        conn = self.get_connection(toloka_conn_id)
        extras = conn.extra_dejson
        self.toloka_token = extras.get("extra__toloka__token") or None
        self.toloka_env = extras.get("extra__toloka__environment") or 'production'
        self.get_conn()

    def get_conn(self) -> TolokaClient:
        """Function that creates a new TolokaClient with token and returns it"""
        if not self.client:
            self.log.debug('Creating toloka client for conn_id: %s', self.toloka_conn_id)

            if not self.toloka_conn_id:
                raise AirflowException('Failed to create toloka client. No toloka_conn_id provided')

            try:
                self.client = TolokaClient(
                    token=self.toloka_token,
                    environment=self.toloka_env,
                )
            except ValueError as toloka_error:
                raise AirflowException(f'Failed to create toloka client, toloka error: {str(toloka_error)}')
            except Exception as e:
                raise AirflowException(f'Failed to create toloka client, error: {str(e)}')

        return self.client

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField, validators, ValidationError

        def check_env(form, field):
            if field.data.lower() not in ['production', 'sandbox']:
                raise ValidationError('Environment field must be production or sandbox')

        return {
            'extra__toloka__token': PasswordField(
                lazy_gettext('Token'),
                [validators.required()],
                widget=BS3PasswordFieldWidget(),
            ),
            'extra__toloka__environment': StringField(
                lazy_gettext('Environment'),
                [check_env],
                widget=BS3TextFieldWidget(),
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        return {
            'hidden_fields': ['port', 'host', 'login', 'schema', 'extra', 'password'],
            'relabeling': {},
            'placeholders': {
                'extra__toloka__token': 'Toloka OAuth token',
                'extra__toloka__environment': 'production or sandbox',
            },
        }
