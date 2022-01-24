from .util import _proceed_func, _update_signature
from functools import wraps


def with_s3_hook(*, connection_id='aws_default', bucket_name):
    """Handles inputs and outputs for custom tasks.

    Substitutes input arguments with objects from s3 where argument is s3 key.
    Loads all output arguments of function to s3 and replace output with corresponding s3 keys.

    Args:
        connection_id: name of airflow connection.
        bucket_name: name of s3 bucket.

    Example:
        Inputs of function that decorated with @with_s3_hook can be simple arguments returned by other tasks
        (tasks_2 example) or s3 keys represented as strings (tasks example).
        Output always will be s3 key that pointing to output stored in s3.

        >>> from airflow.decorators import task
        >>>
        >>> @task
        >>> @with_s3_hook(bucket_name=bucket)
        >>> def prepare_datasets(url: str):
        >>>     import pandas as pd
        >>>
        >>>     main_tasks = pd.read_csv(url).sample(n=100)
        >>>
        >>>     return main_tasks
        >>>
        >>> @task
        >>> def prepare_datasets_2():
        >>>     return {'headline': ['some_value', 'another_value']}
        >>>
        >>> @task
        >>> @with_s3_hook(bucket_name=bucket)
        >>> def prepare_tasks(main_tasks):
        >>>     from toloka.client import Task
        >>>
        >>>     return [Task(input_values={'headline': headline})
        >>>             for headline in main_tasks['headline']]
        >>>
        >>> # main_tasks here is a s3 key
        >>> main_tasks = prepare_datasets(url='https://raw.githubusercontent.com/Pocoder/toloka-airflow/main/data/not_known.csv')
        >>> # tasks here is a s3 key
        >>> tasks = prepare_tasks(main_tasks)
        >>>
        >>> # main_tasks_2 here is default dict
        >>> main_tasks_2 = prepare_datasets_2()
        >>> # tasks_2 here is a s3 key too
        >>> tasks_2 = prepare_tasks(main_tasks_2)

        Further function which will take tasks or tasks_2 as argument should be wrapped in with_s3_hook to automatically
        handle input or should get data from s3 manually.
        ...
    """

    def wrapper(func):

        def handle_hook(*args, **kwargs):

            return _proceed_func(func, connection_id, bucket_name, *args, **kwargs)

        return _update_signature(func, handle_hook)

    return wrapper


def _with_s3_hook(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'intermediary' in kwargs:
            config = kwargs['intermediary']

            storage_type = config.get('type', 's3')
            conn_id = config.get('conn_id', 'aws_default')

            try:
                bucket_name = config['bucket_name']
            except KeyError:
                raise ValueError('You have to provide bucket_name for intermediary storage')

            if storage_type == 's3':
                return with_s3_hook(connection_id=conn_id, bucket_name=bucket_name)(func)(*args, **kwargs)
            else:
                raise NotImplementedError('We can not work with this storage yet.')
        else:
            return func(*args, **kwargs)

    return wrapper

