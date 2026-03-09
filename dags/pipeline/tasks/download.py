from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os

STORAGE_DIR_ON_HOST = os.getenv("CITYJSON_STORAGE_DIR", "/opt/airflow/data/storage")

# This method downloads a file from S3 to the host.
def download_from_s3(bucket: str, key: str, dest: str):
    hook = S3Hook(aws_conn_id="aws_default")
    obj = hook.get_key(key, bucket_name=bucket)
    if obj is None:
        raise FileNotFoundError(f"s3://{bucket}/{key} not found")
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    obj.download_file(dest)


def download_task_callable(**context):
    params = context["params"]
    if "bucket" not in params:
        raise ValueError(f"Missing required parameter: bucket")
    if "key" not in params:
        raise ValueError(f"Missing required parameter: key")
    if "target_filename" not in params:
        raise ValueError(f"Missing required parameter: target_filenname")

    dest_dir = params.get("dest_dir") or STORAGE_DIR_ON_HOST
    dest_path = os.path.join(dest_dir, params["target_filename"])
    download_from_s3(
        bucket=params["bucket"],
        key=params["key"],
        dest=dest_path,
    )


def download() -> PythonOperator:
    return PythonOperator(
        task_id="download_file_task",
        python_callable=download_task_callable,
        provide_context=True,
    )
    