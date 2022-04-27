from typing import Dict, Union, Callable
import logging

from airflow.sensors.python import PythonSensor

from toloka.client import Pool
from toloka.client.analytics_request import CompletionPercentagePoolAnalytics
from toloka.util._managing_headers import add_headers

from ..hooks.toloka import TolokaHook
from ..utils import extract_id

logger = logging.getLogger(__name__)


@add_headers('airflow')
def _is_pool_finished(pool: Union[Pool, Dict, str], toloka_conn_id: str) -> bool:
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    pool_id = extract_id(pool, Pool)
    pool = toloka_client.get_pool(pool_id)

    op = toloka_client.get_analytics([CompletionPercentagePoolAnalytics(subject_id=pool_id)])
    percentage = toloka_client.wait_operation(op).details['value'][0]['result']['value']
    logger.info(f'Pool {pool_id} - {percentage}%')

    return pool.is_closed()


def wait_pool(
    pool: Union[Pool, Dict, str],
    mode: str = 'reschedule',
    poke_interval: float = 60,
    timeout: float = 60*60,
    on_failure_callback: Callable = None,
    *,
    toloka_conn_id: str = 'toloka_default',
) -> PythonSensor:
    """Wait given pool until close.
        Args:
            pool: Either a `Pool` object or it's config or a pool_id value.
            mode: Mode of Airflow sensor to be used.
            poke_interval: `timedelta` interval between checks. One minute by default.
            timeout: Time, in seconds before the task times out and fails.
            on_failure_callback: Function to be called when sensor fails.
            toloka_conn_id: Airflow connection with toloka credentials.
            intermediary: Configuration of storage for passing data.
        Returns:
            PythonSensor that checks if pool is closed.
    """
    return PythonSensor(
        task_id='wait_pool',
        python_callable=_is_pool_finished,
        op_args=[pool, toloka_conn_id],
        mode=mode,
        poke_interval=poke_interval,
        timeout=timeout,
        on_failure_callback=on_failure_callback,
    )
