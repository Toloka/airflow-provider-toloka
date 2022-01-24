import pandas as pd
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Union

from airflow.decorators import task

from toloka.client import Assignment, Pool, Project, Task, TolokaClient, Training
from toloka.client.analytics_request import CompletionPercentagePoolAnalytics
from toloka.client.batch_create_results import TaskBatchCreateResult
from toloka.client import add_headers

from .hooks import TolokaHook
from .util import _structure_from_conf, _extract_id
from .hook_decorators import _with_s3_hook

logger = logging.getLogger(__name__)


@task
@_with_s3_hook
@add_headers('airflow')
def create_project(
    obj: Union[Project, Dict, str, bytes],
    *,
    toloka_conn_id: str = 'toloka_default',
    intermediary: Optional[dict] = None,
) -> Project:
    """Create a Project object from given config.
    Args:
        obj: Either a `Project` object itself or a config to make a `Project`.
        toloka_conn_id: toloka credentials
        intermediary: configuration of storage for passing data
    Returns:
        Project object.
    """
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    obj = _structure_from_conf(obj, Project)
    project = toloka_client.create_project(obj)
    if intermediary is None:
        return project.to_json()
    else:
        return project


@task
@_with_s3_hook
@add_headers('airflow')
def create_exam_pool(
    obj: Union[Training, Dict, str, bytes],
    *,
    project: Union[Project, str, None] = None,
    toloka_conn_id: str = 'toloka_default',
    intermediary: Optional[dict] = None,
) -> Training:
    """Create a Training pool object from given config.
    Args:
        obj: Either a `Training` object itself or a config to make a `Training`.
        project: Project to assign a training pool to. May pass either an object, config or project_id.
        toloka_conn_id: toloka credentials
        intermediary: configuration of storage for passing data
    Returns:
        Training object.
    """
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    obj = _structure_from_conf(obj, Training)
    if project is not None:
        obj.project_id = _extract_id(project, Project)
    training = toloka_client.create_training(obj)
    if intermediary is None:
        return training.to_json()
    else:
        return training


@task
@_with_s3_hook
@add_headers('airflow')
def create_pool(
    obj: Union[Pool, Dict, str, bytes],
    *,
    project: Union[Project, str, None] = None,
    exam_pool: Union[Training, str, None] = None,
    expiration: Union[datetime, timedelta, None] = None,
    toloka_conn_id: str = 'toloka_default',
    intermediary: Optional[dict] = None,
) -> Pool:
    """Create a Pool object from given config.
    Args:
        obj: Either a `Pool` object itself or a config to make a `Pool`.
        project: Project to assign a pool to. May pass either an object, config or project_id.
        exam_pool: Related training pool. May pass either an object, config or pool_id.
        expiration: Expiration setting. May pass any of:
            * `None` if this setting if already present;
            * `datetime` object to set exact datetime;
            * `timedelta` to set expiration related to the current time.
        toloka_conn_id: toloka credentials
        intermediary: configuration of storage for passing data
    Returns:
        Pool object.
    """
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    obj = _structure_from_conf(obj, Pool)
    if project is not None:
        obj.project_id = _extract_id(project, Project)
    if exam_pool:
        if obj.quality_control.training_requirement is None:
            raise ValueError('pool.quality_control.training_requirement should be set before exam_pool assignment')
        obj.quality_control.training_requirement.training_pool_id = _extract_id(exam_pool, Training)
    if expiration:
        obj.will_expire = datetime.now() + expiration if isinstance(expiration, timedelta) else expiration
    pool = toloka_client.create_pool(obj)
    if intermediary is None:
        return pool.to_json()
    else:
        return pool


@task
@_with_s3_hook
@add_headers('airflow')
def create_tasks(
    tasks: List[Union[Task, Dict]],
    *,
    pool: Union[Pool, Training, Dict, str, None] = None,
    toloka_conn_id: str = 'toloka_default',
    intermediary: Optional[dict] = None,
    kwargs: Optional[Dict] = None
) -> TaskBatchCreateResult:
    """Create a list of tasks for a given pool.
    Args:
        tasks: List of either a `Task` objects or a task conofigurations.
        toloka_conn_id: toloka credentials
        pool: Allow to set tasks pool if it's not present in the tasks themselves.
            May be either a `Pool` or `Training` object or config or a pool_id value.
        intermediary: configuration of storage for passing data
        kwargs: Any other args presented in `toloka.client.task.CreateTasksParameters`.
    Returns:
        toloka.client.batch_create_results.TaskBatchCreateResult object.
    """
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    if kwargs is None:
        kwargs = {}
    tasks = [_structure_from_conf(task, Task) for task in tasks]
    if pool is not None:
        try:
            pool_id = _extract_id(pool, Pool)
        except Exception:
            pool_id = _extract_id(pool, Training)
        for task in tasks:
            task.pool_id = pool_id
    tasks = toloka_client.create_tasks(tasks, **kwargs)
    logger.info(f'Tasks: {tasks} created')


@task
@_with_s3_hook
@add_headers('airflow')
def open_pool(
    obj: Union[Pool, str],
    *,
    toloka_conn_id: str = 'toloka_default',
    intermediary: Optional[dict] = None,
) -> Pool:
    """Open given pool.
    Args:
        obj: Pool id or `Pool` object of it's config.
        toloka_conn_id: toloka credentials
        intermediary: configuration of storage for passing data
    Returns:
        Pool object.
    """
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    obj = _structure_from_conf(obj, Pool)
    pool = toloka_client.open_pool(obj)
    if intermediary is None:
        return pool.to_json()
    else:
        return pool


@task
@_with_s3_hook
@add_headers('airflow')
def open_exam_pool(
    obj: Union[Training, str],
    *,
    toloka_conn_id: str = 'toloka_default',
    intermediary: Optional[dict] = None,
) -> Pool:
    """Open given training pool.
    Args:
        obj: Training pool_id or `Training` object of it's config.
        toloka_conn_id: toloka credentials
        intermediary: configuration of storage for passing data
    Returns:
        Training object.
    """
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    obj = _structure_from_conf(obj, Training)
    training = toloka_client.open_training(obj)
    if intermediary is None:
        return training.to_json()
    else:
        return training


@task
@_with_s3_hook
@add_headers('airflow')
def wait_pool(
    pool: Union[Pool, Dict, str],
    period: timedelta = timedelta(seconds=60),
    *,
    open_pool: bool = False,
    toloka_conn_id: str = 'toloka_default',
    intermediary: Optional[dict] = None,
) -> Pool:
    """Wait given pool until close.
    Args:
        pool: Either a `Pool` object or it's config or a pool_id value.
        period: `timedelta` interval between checks. One minute by default.
        open_pool: Allow to open pool at start if it's closed. False by default.
        toloka_conn_id: toloka credentials
        intermediary: configuration of storage for passing data
    Returns:
        Pool object.
    """
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    pool_id = _extract_id(pool, Pool)
    pool = toloka_client.get_pool(pool_id)
    if pool.is_closed() and open_pool:
        pool = toloka_client.open_pool(pool_id)

    while pool.is_open():
        op = toloka_client.get_analytics([CompletionPercentagePoolAnalytics(subject_id=pool_id)])
        percentage = toloka_client.wait_operation(op).details['value'][0]['result']['value']
        logger.info(f'Pool {pool_id} - {percentage}%')

        time.sleep(period.total_seconds())
        pool = toloka_client.get_pool(pool_id)

    if intermediary is None:
        return pool.to_json()
    else:
        return pool


@task
@_with_s3_hook
@add_headers('airflow')
def get_assignments(
    pool: Union[Pool, Dict, str],
    status: Union[str, List[str], Assignment.Status, List[Assignment.Status], None] = None,
    *,
    toloka_conn_id: str = 'toloka_default',
    intermediary: Optional[dict] = None,
    kwargs: Optional[Dict] = None
) -> List[Assignment]:
    """Get all assignments of selected status from pool.
    Args:
        pool: Either a `Pool` object or it's config or a pool_id.
        status: A status or a list of statuses to get. All statuses (None) by default.
        toloka_conn_id: toloka credentials
        intermediary: configuration of storage for passing data
        kwargs: Any other args presented in `toloka.client.search_requests.AssignmentSearchRequest`.
    Returns:
        List of `Assignment` objects.
    """
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    if kwargs is None:
        kwargs = {}
    pool_id = _extract_id(pool, Pool)
    assignments = toloka_client.get_assignments(pool_id=pool_id, status=status, **kwargs)
    if intermediary is None:
        return [assignment.to_json() for assignment in assignments]
    else:
        return list(assignments)


@task
@_with_s3_hook
@add_headers('airflow')
def get_assignments_df(
    pool: Union[Pool, Dict, str],
    status: Union[str, List[str], Assignment.Status, List[Assignment.Status], None] = None,
    *,
    toloka_conn_id: str = 'toloka_default',
    intermediary: Optional[dict] = None,
    kwargs: Optional[Dict] = None
) -> pd.DataFrame:
    """Get pool assignments of selected statuses in Pandas DataFrame format useful for aggregation.
    Args:
        pool: Either a `Pool` object or it's config or a pool_id.
        status: A status or a list of statuses to get. All statuses (None) by default.
        toloka_conn_id: toloka credentials
        intermediary: configuration of storage for passing data
        kwargs: Any other args presented in `toloka.client.assignment.GetAssignmentsTsvParameters`.
    Returns:
        DataFrame with selected assignments. Note that nested paths are presented with a ":" sign.
    """
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    if kwargs is None:
        kwargs = {}
    pool_id = _extract_id(pool, Pool)
    if not status:
        status = []
    elif isinstance(status, (str, Assignment.Status)):
        status = [status]
    if intermediary is None:
        return toloka_client.get_assignments_df(pool_id=pool_id, status=status, **kwargs).to_json()
    else:
        return toloka_client.get_assignments_df(pool_id=pool_id, status=status, **kwargs)
