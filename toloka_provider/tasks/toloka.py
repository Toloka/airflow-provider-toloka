"""
Module contains airflow tasks to make basic manipulations in Toloka such as project and pool processing.
"""
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Union

from airflow.decorators import task

from toloka.client import Assignment, Pool, Project, Task, Training
from toloka.util._managing_headers import add_headers

from ..hooks.toloka import TolokaHook
from ..utils import serialize_if_default_xcom_backend, structure_from_conf, extract_id

logger = logging.getLogger(__name__)


@task
@serialize_if_default_xcom_backend
@add_headers('airflow')
def create_project(
    obj: Union[Project, Dict, str, bytes],
    *,
    toloka_conn_id: str = 'toloka_default',
) -> Union[Project, str]:
    """Create a Project object from given config.
    Args:
        obj: Either a `Project` object itself or a config to make a `Project`.
        toloka_conn_id: Airflow connection with toloka credentials.
    Returns:
        Project object if custom XCom backend is configured or its JSON serialized version otherwise.
    """
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    obj = structure_from_conf(obj, Project)
    return toloka_client.create_project(obj)


@task
@serialize_if_default_xcom_backend
@add_headers('airflow')
def create_exam_pool(
    obj: Union[Training, Dict, str, bytes],
    *,
    project: Union[Project, str, None] = None,
    toloka_conn_id: str = 'toloka_default',
) -> Union[Training, str]:
    """Create a Training pool object from given config.
    Args:
        obj: Either a `Training` object itself or a config to make a `Training`.
        project: Project to assign a training pool to. May pass either an object, config or project_id.
        toloka_conn_id: Airflow connection with toloka credentials.
    Returns:
        Training object if custom XCom backend is configured or its JSON serialized version otherwise.
    """
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    obj = structure_from_conf(obj, Training)
    if project is not None:
        obj.project_id = extract_id(project, Project)
    return toloka_client.create_training(obj)


@task
@serialize_if_default_xcom_backend
@add_headers('airflow')
def create_pool(
    obj: Union[Pool, Dict, str, bytes],
    *,
    project: Union[Project, str, None] = None,
    exam_pool: Union[Training, str, None] = None,
    expiration: Union[datetime, timedelta, None] = None,
    toloka_conn_id: str = 'toloka_default',
) -> Union[Pool, str]:
    """Create a Pool object from given config.
    Args:
        obj: Either a `Pool` object itself or a config to make a `Pool`.
        project: Project to assign a pool to. May pass either an object, config or project_id.
        exam_pool: Related training pool. May pass either an object, config or pool_id.
        expiration: Expiration setting. May pass any of:
            * `None` if this setting is already present;
            * `datetime` object to set exact datetime;
            * `timedelta` to set expiration related to the current time.
        toloka_conn_id: Airflow connection with toloka credentials.
    Returns:
        Pool object if custom XCom backend is configured or its JSON serialized version otherwise.
    """
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    obj = structure_from_conf(obj, Pool)
    if project is not None:
        obj.project_id = extract_id(project, Project)
    if exam_pool:
        if obj.quality_control.training_requirement is None:
            raise ValueError('pool.quality_control.training_requirement should be set before exam_pool assignment')
        obj.quality_control.training_requirement.training_pool_id = extract_id(exam_pool, Training)
    if expiration:
        obj.will_expire = datetime.now() + expiration if isinstance(expiration, timedelta) else expiration
    return toloka_client.create_pool(obj)


@task
@serialize_if_default_xcom_backend
@add_headers('airflow')
def create_tasks(
    tasks: List[Union[Task, Dict]],
    *,
    pool: Union[Pool, Training, Dict, str, None] = None,
    toloka_conn_id: str = 'toloka_default',
    additional_args: Optional[Dict] = None,
) -> None:
    """Create a list of tasks for a given pool.
    Args:
        tasks: List of either a `Task` objects or a task conofigurations.
        pool: Allow to set tasks pool if it's not present in the tasks themselves.
            May be either a `Pool` or `Training` object or config or a pool_id value.
        toloka_conn_id: Airflow connection with toloka credentials.
        additional_args: Any other args presented in `toloka.client.task.CreateTasksParameters`.
    """
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    if additional_args is None:
        additional_args = {}
    tasks = [structure_from_conf(task, Task) for task in tasks]
    if pool is not None:
        try:
            pool_id = extract_id(pool, Pool)
        except Exception:
            pool_id = extract_id(pool, Training)
        for task in tasks:
            task.pool_id = pool_id
    tasks = toloka_client.create_tasks(tasks, **additional_args)
    logger.info(f'Tasks: {tasks} created')


@task
@serialize_if_default_xcom_backend
@add_headers('airflow')
def open_pool(
    obj: Union[Pool, str],
    *,
    toloka_conn_id: str = 'toloka_default',
) -> Union[Pool, str]:
    """Open given pool.
    Args:
        obj: Pool id or `Pool` object of it's config.
        toloka_conn_id: Airflow connection with toloka credentials.
    Returns:
        Pool object if custom XCom backend is configured or its JSON serialized version otherwise.
    """
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    pool = structure_from_conf(obj, Pool)
    pool_id = extract_id(pool, Pool)
    return toloka_client.open_pool(pool_id)


@task
@serialize_if_default_xcom_backend
@add_headers('airflow')
def open_exam_pool(
    obj: Union[Training, str],
    *,
    toloka_conn_id: str = 'toloka_default',
) -> Union[Pool, str]:
    """Open given training pool.
    Args:
        obj: Training pool_id or `Training` object of it's config.
        toloka_conn_id: Airflow connection with toloka credentials.
    Returns:
        Training object if custom XCom backend is configured or its JSON serialized version otherwise.
    """
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    training = structure_from_conf(obj, Training)
    training_id = extract_id(training, Training)
    return toloka_client.open_training(training_id)


@task
@serialize_if_default_xcom_backend
@add_headers('airflow')
def get_assignments(
    pool: Union[Pool, Dict, str],
    status: Union[str, List[str], Assignment.Status, List[Assignment.Status], None] = None,
    *,
    toloka_conn_id: str = 'toloka_default',
    additional_args: Optional[Dict] = None,
) -> List[Union[Assignment, str]]:
    """Get all assignments of selected status from pool.
    Args:
        pool: Either a `Pool` object or it's config or a pool_id.
        status: A status or a list of statuses to get. All statuses (None) by default.
        toloka_conn_id: Airflow connection with toloka credentials.
        additional_args: Any other args presented in `toloka.client.search_requests.AssignmentSearchRequest`.
    Returns:
        List of `Assignment` objects if custom XCom backend is configured or of its JSON serialized versions otherwise.
    """
    toloka_hook = TolokaHook(toloka_conn_id=toloka_conn_id)
    toloka_client = toloka_hook.get_conn()

    if additional_args is None:
        additional_args = {}
    pool_id = extract_id(pool, Pool)
    assignments = toloka_client.get_assignments(pool_id=pool_id, status=status, **additional_args)
    return list(assignments)
