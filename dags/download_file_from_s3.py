from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import os


# This method downloads a file from S3 to the host.
def download_from_s3(bucket: str, key: str, dest: str):
    hook = S3Hook(aws_conn_id="aws_default")
    obj = hook.get_key(key, bucket_name=bucket)
    if obj is None:
        raise FileNotFoundError(f"s3://{bucket}/{key} not found")
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    obj.download_file(dest)


DAG_ID = "download_file_from_s3"
STORAGE_DIR_ON_HOST = os.getenv("CITYJSON_STORAGE_DIR", "/opt/airflow/data/storage")

# This is a DAG which downloads a file from S3 to the host.
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["S3", "Host", "Download"],
    params={
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
    },
) as dag:

    def download_task_callable(**context):
        params = context["params"]
        dest_dir = params.get("dest_dir") or STORAGE_DIR_ON_HOST
        dest_path = os.path.join(dest_dir, params["target_filename"])
        download_from_s3(
            bucket=params["bucket"],
            key=params["key"],
            dest=dest_path,
        )

    download = PythonOperator(
        task_id="download_file_task",
        python_callable=download_task_callable,
        provide_context=True,
    )