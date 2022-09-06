"""
Module contains sensor to wait pool finishing
"""
from typing import Dict, Union, Sequence, Iterable, Optional

from airflow.sensors.base import BaseSensorOperator
from toloka.client import Pool, AppProject, AppBatch
from toloka.client.analytics_request import CompletionPercentagePoolAnalytics

from ..hooks.toloka import TolokaHook
from ..utils import extract_id


class WaitPoolSensor(BaseSensorOperator):
    """
    Wait given pool until close.

    :param toloka_pool: Either a `Pool` object or it's config or a pool_id value.
    :param toloka_conn_id: Airflow connection with toloka credentials.
    :param success_on_reasons: Container of Pool.CloseReason. `WaitPoolSensor`
        will wait until both `pool` becomes closed and `success_on_reasons`
        contains `pool.last_close_reason`.
    """

    template_fields: Sequence[str] = ('toloka_pool', 'toloka_conn_id', 'success_on_reasons')

    def __init__(
        self,
        *,
        toloka_pool: Union[Pool, Dict, str],
        toloka_conn_id: str = 'toloka_default',
        success_on_reasons: Optional[Iterable[Union[Pool.CloseReason, str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.toloka_pool = toloka_pool
        self.toloka_conn_id = toloka_conn_id
        self.success_on_reasons = [Pool.CloseReason(
            reason) for reason in success_on_reasons] if success_on_reasons is not None else list(Pool.CloseReason)

    def poke(self, context: 'Context') -> bool:
        """Checks if pool is closed"""
        pool_id = extract_id(self.toloka_pool, Pool)
        toloka_hook = TolokaHook(toloka_conn_id=self.toloka_conn_id)
        toloka_client = toloka_hook.get_conn()

        pool = toloka_client.get_pool(pool_id)

        op = toloka_client.get_analytics([CompletionPercentagePoolAnalytics(subject_id=pool_id)])
        percentage = toloka_client.wait_operation(op).details['value'][0]['result']['value']
        self.log.info(f'Pool {pool_id} - {percentage}%')

        return pool.is_closed() and pool.last_close_reason in self.success_on_reasons


class WaitAppBatchSensor(BaseSensorOperator):
    """
    Wait given App batch until it enters an of success_on_statuses status.

    :param app_project: Either an `AppProject` object or it's config or an app_project_id value.
    :param batch: Either an `AppBatch` object or it's config or a batch_id value.
    :param toloka_conn_id: Airflow connection with toloka credentials.
    :param success_on_statuses: Container of AppBatch.Status. `WaitAppBatchSensor` will wait until
        `success_on_statuses` includes `batch.status`.
    """

    template_fields: Sequence[str] = ('app_project', 'batch', 'toloka_conn_id', 'success_on_statuses')

    def __init__(
        self,
        *,
        app_project: Union[AppProject, Dict, str],
        batch: Union[AppBatch, Dict, str],
        toloka_conn_id: str = 'toloka_default',
        success_on_statuses: Iterable[Union[AppBatch.Status, str]] = (AppBatch.Status.COMPLETED,),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.app_project = app_project
        self.batch = batch
        self.toloka_conn_id = toloka_conn_id
        self.success_on_statuses = [AppBatch.Status(status) for status in success_on_statuses]

    def poke(self, context: 'Context') -> bool:
        """Checks if batch status in success_on_statuses"""
        app_project_id = extract_id(self.app_project, AppProject)
        app_batch_id = extract_id(self.batch, AppBatch)
        toloka_hook = TolokaHook(toloka_conn_id=self.toloka_conn_id)
        toloka_client = toloka_hook.get_conn()

        batch = toloka_client.get_app_batch(app_project_id, app_batch_id)
        processed_fraction = batch.items_processed_count / batch.items_count if batch.items_count > 0 else 0.
        self.log.info(f'Batch {batch.id} - {processed_fraction * 100}%')
        return batch.status in self.success_on_statuses
