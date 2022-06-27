"""
Module contains sensor to wait pool finishing
"""
from typing import Dict, Union, Callable, Sequence
import logging

from airflow.sensors.python import PythonSensor
from airflow.sensors.base import BaseSensorOperator

from toloka.client import Pool
from toloka.client.analytics_request import CompletionPercentagePoolAnalytics
from toloka.util._managing_headers import add_headers

from ..hooks.toloka import TolokaHook
from ..utils import extract_id


class WaitPoolSensor(BaseSensorOperator):
    """
    Wait given pool until close.

    :param pool: Either a `Pool` object or it's config or a pool_id value.
    :param toloka_conn_id: Airflow connection with toloka credentials.
    """

    template_fields: Sequence[str] = ('toloka_pool',)

    def __init__(
        self,
        *,
        toloka_pool: Union[Pool, Dict, str],
        toloka_conn_id: str = 'toloka_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.toloka_pool = toloka_pool
        self.toloka_conn_id = toloka_conn_id

    def poke(self, context: 'Context') -> bool:
        pool_id = extract_id(self.toloka_pool, Pool)
        toloka_hook = TolokaHook(toloka_conn_id=self.toloka_conn_id)
        toloka_client = toloka_hook.get_conn()

        pool = toloka_client.get_pool(pool_id)

        op = toloka_client.get_analytics([CompletionPercentagePoolAnalytics(subject_id=pool_id)])
        percentage = toloka_client.wait_operation(op).details['value'][0]['result']['value']
        self.log.info(f'Pool {pool_id} - {percentage}%')

        return pool.is_closed()
