from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk.definitions.param import ParamsDict
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


def download_task_callable(params):
    required_parameters = ["bucket", "key", "dest_dir", "target_filename"]
    for rq in required_parameters:
        if not params.get(rq):
            raise ValueError(f"Missing parameter: {rq}")

    dest_dir = params.get("dest_dir") or STORAGE_DIR_ON_HOST
    dest_path = os.path.join(dest_dir, params["target_filename"])
    download_from_s3(
        bucket=params["bucket"],
        key=params["key"],
        dest=dest_path,
    )


def download(params: ParamsDict) -> PythonOperator:
    return PythonOperator(
        task_id="download_file_task",
        python_callable=download_task_callable,
        op_kwargs={'params': params},
    )
    
"""
params=ParamsDict({
        "bucket": Param(
            default="",
            type="string",
            description="Name of the S3 bucket containing the object",
        ),
        "key": Param(
            default="",
            type="string",
            description="Key (path) of the object in the bucket",
        ),
        "dest_dir": Param(
            default="",
            type="string",
            description="Host directory to save the downloaded file (defaults to storage dir)",
        ),
        "target_filename": Param(
            default="downloaded_file.zip",
            type="string",
            description="Filename to use for the downloaded object",
        ),
    }),
"""