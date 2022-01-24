import inspect
import json
import pickle
from functools import wraps
from typing import Any, Callable, Iterable, Type
import uuid

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from toloka.client import TolokaClient, structure


def _update_signature(
    func: Callable,
    wrapper: Callable,
    *,
    add_wrapper_args: Iterable[str] = (),
    remove_func_args: Iterable[str] = (),
) -> Callable:
    """Provides better function signatures by run-time updating func by adding/removing wrapper parameters."""

    remove_func_args = set(remove_func_args)

    signature_func = inspect.signature(func)
    parameters_keep = [
        param
        for param in signature_func.parameters.values()
        if param.name not in remove_func_args and param.kind != inspect.Parameter.VAR_KEYWORD
    ]

    parameters_wrapper = inspect.signature(wrapper).parameters
    parameters_add = [parameters_wrapper[arg_name] for arg_name in add_wrapper_args]

    last_param = next(reversed(signature_func.parameters.values()), inspect.Parameter)
    if last_param.kind == inspect.Parameter.VAR_KEYWORD:
        parameters_add.append(last_param)

    res = wraps(func)(wrapper)
    res.__signature__ = signature_func.replace(parameters=parameters_keep + parameters_add)

    return res


def _handle_input(hook, bucket, *args, **kwargs):
    input_list = []
    for arg in args:
        if isinstance(arg, str) and hook.check_for_key(arg, bucket_name=bucket):
            data_obj = hook.get_key(key=arg, bucket_name=bucket)
            data = data_obj.get()['Body'].read()
            downloaded = pickle.loads(data)
            input_list.append(downloaded)
        else:
            input_list.append(arg)

    input_dict = {}
    for key, value in kwargs.items():
        if isinstance(value, str) and hook.check_for_key(value, bucket_name=bucket):
            data_obj = hook.get_key(key=value, bucket_name=bucket)
            data = data_obj.get()['Body'].read()
            downloaded = pickle.loads(data)
            input_dict[key] = downloaded
        else:
            input_dict[key] = value

    return input_list, input_dict


def _handle_output(hook, bucket, result):
    if isinstance(result, dict):
        for key, value in result.items():
            key_for_value = "data_" + str(uuid.uuid4())

            hook.load_bytes(
                bytes_data=pickle.dumps(value),
                key=key_for_value,
                bucket_name=bucket,
                replace=True
            )

            result[key] = key_for_value
    else:
        key_for_value = "data_" + str(uuid.uuid4())

        hook.load_bytes(
            bytes_data=pickle.dumps(result),
            key=key_for_value,
            bucket_name=bucket,
            replace=True
        )
        result = key_for_value

    return result


def _proceed_func(func, conn_id, bucket, *args, **kwargs):
    s3_hook = S3Hook(aws_conn_id=conn_id)

    input_list, input_dict = _handle_input(s3_hook, bucket, *args, **kwargs)
    result = func(*input_list, **input_dict)
    result = _handle_output(s3_hook, bucket, result)

    return result


def _structure_from_conf(obj: Any, cl: Type) -> object:
    if isinstance(obj, cl):
        return obj
    if isinstance(obj, bytes):
        try:
            return pickle.loads(obj)
        except Exception:
            pass
        obj = obj.decode()
    if isinstance(obj, str):
        obj = json.loads(obj)
    return structure(obj, cl)


def _extract_id(obj: Any, cl: Type) -> str:
    if isinstance(obj, str):
        try:
            obj = json.loads(obj)
        except Exception:
            return obj
    return _structure_from_conf(obj, cl).id
