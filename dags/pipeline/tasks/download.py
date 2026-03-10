from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
from pipeline.config import WORK_DIR


def download_from_s3(bucket: str, key: str, dest: str):
    hook = S3Hook(aws_conn_id="aws_default")
    obj = hook.get_key(key, bucket_name=bucket)
    if obj is None:
        raise FileNotFoundError(f"s3://{bucket}/{key} not found")
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    obj.download_file(dest)


def _download_callable(params):
    bucket = params.get("bucket")
    key = params.get("key")
    if not bucket:
        raise ValueError("Missing param: bucket")
    if not key:
        raise ValueError("Missing param: key")
    dest = os.path.join(WORK_DIR, "zip", key)
    download_from_s3(bucket, key, dest)


def make_download_task() -> PythonOperator:
    return PythonOperator(
        task_id="download_file_task",
        python_callable=_download_callable,
    )