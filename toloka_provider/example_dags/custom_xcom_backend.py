from typing import Any
import pickle
import uuid

from airflow.models.xcom import BaseXCom
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from toloka.client.primitives.base import BaseTolokaObject


class TolokaS3XComBackend(BaseXCom):
    PREFIX = 'xcom_s3://'
    BUCKET_NAME = 'yourbucketname'

    @staticmethod
    def serialize_value(value: Any):
        if isinstance(value, BaseTolokaObject):
            value = value.to_json().encode()
            hook = S3Hook()
            key_for_value = 'data_' + str(uuid.uuid4())
            hook.load_bytes(
                bytes_data=value,
                key=key_for_value,
                bucket_name=TolokaS3XComBackend.BUCKET_NAME,
                replace=True
            )
            value = TolokaS3XComBackend.PREFIX + key_for_value
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result) -> Any:
        local_result = BaseXCom.deserialize_value(result)

        if isinstance(local_result, str) and local_result.startswith(TolokaS3XComBackend.PREFIX):
            hook = S3Hook()
            key = local_result.replace(TolokaS3XComBackend.PREFIX, '')
            data_obj = hook.get_key(key=key, bucket_name=TolokaS3XComBackend.BUCKET_NAME)
            data = data_obj.get()['Body'].read()
            local_result = data.decode()
        return local_result

