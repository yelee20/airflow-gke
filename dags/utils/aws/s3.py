import json
from datetime import date
from typing import Dict, Any, List, Union

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def get_sourcing_path(
        provider: str,
        data_category: str,
        execution_date: Union[str, date],
) -> str:
    return f"{provider}/_tmp/{data_category}/{execution_date}"


def upload_json_to_s3(
        s3_hook: S3Hook,
        bucket_name: str,
        data_key: str,
        json_data: Union[Dict[str, Any], List[Any]],
        replace: bool = True,
) -> None:
    s3_hook.load_string(
        string_data=json.dumps(json_data, ensure_ascii=False),
        key=data_key,
        bucket_name=bucket_name,
        replace=replace,
    )


def upload_bytes_to_s3(
        s3_hook: S3Hook,
        bucket_name: str,
        data_key: str,
        bytes_data: bytes,
        replace: bool = True,
) -> None:
    s3_hook.load_bytes(
        bytes_data=bytes_data,
        key=data_key,
        bucket_name=bucket_name,
        replace=replace,
    )


def delete_already_existing_data(
        s3_hook: S3Hook,
        bucket_name: str,
        s3_path: str,
        delimiter: str = "/",
) -> None:
    if s3_path[-1] != delimiter:
        s3_path = f"{s3_path}{delimiter}"
    keys = s3_hook.list_keys(
        bucket_name=bucket_name, prefix=s3_path, delimiter=delimiter
    )
    if keys:
        s3_hook.delete_objects(
            bucket=bucket_name,
            keys=keys,
        )
