from __future__ import annotations

import os
import zipfile
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

import boto3
from botocore.config import Config


# host directories that are visible inside the scheduler/container
STORAGE_DIR_ON_HOST = os.getenv("CITYJSON_STORAGE_DIR", "/opt/airflow/data/storage")

# S3 configuration (match values used in s3-gui and .env.example)
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
S3_REGION = os.getenv("S3_REGION", "eu-central-1")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "test")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "test")
S3_BUCKET = os.getenv("S3_BUCKET", "external-downloads")
S3_FORCE_PATH_STYLE = os.getenv("S3_FORCE_PATH_STYLE", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}

# You need this client to talk to the S3.
def _build_s3_client():
    """Create a boto3 client configured according to the environment.

    This duplicates the logic from ``s3-gui/app.py`` so the DAG speaks to
    the same endpoint/bucket.
    """
    addressing_style = "path" if S3_FORCE_PATH_STYLE else "virtual"
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(s3={"addressing_style": addressing_style}),
    )


def _zip_directory(source_dir: str, output_zip: str) -> None:
    """Recursively zip ``source_dir`` into ``output_zip``.

    The archive preserves the directory structure relative to ``source_dir``.
    """
    with zipfile.ZipFile(output_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(source_dir):
            for fname in files:
                full = os.path.join(root, fname)
                # store relative path so the zip is tidy
                arcname = os.path.relpath(full, source_dir)
                zf.write(full, arcname)


def _upload_folder_callable(**context):
    """Airflow callable that zips a folder and uploads it to S3.

    Parameters are supplied via ``dag_run.conf`` or the UI:

    * ``source_dir`` (optional) – path on the host to compress; defaults to
      ``CITYJSON_STORAGE_DIR``.
    * ``s3_prefix`` (optional) – prefix to prepend to the S3 key.
    * ``zip_name`` (optional) – file name to use for the archive; if omitted
      the DAG run id is used.
    """
    params = context["params"]
    source_dir = params.get("source_dir") or STORAGE_DIR_ON_HOST
    if not os.path.isdir(source_dir):
        raise FileNotFoundError(f"Source directory does not exist: {source_dir}")

    run_id = context.get("run_id", "manual")
    zip_name = params.get("zip_name") or f"{run_id}.zip"
    s3_prefix = (params.get("s3_prefix") or "").strip().strip("/")

    # temporary location for the zip inside the scheduler container
    tmp_zip = os.path.join("/tmp", zip_name)

    _zip_directory(source_dir, tmp_zip)
    client = _build_s3_client()

    key = zip_name if not s3_prefix else f"{s3_prefix}/{zip_name}"
    client.upload_file(tmp_zip, S3_BUCKET, key)

    print(f"zipped '{source_dir}' → '{tmp_zip}' and uploaded to s3://{S3_BUCKET}/{key}")


# This is a DAG which zips a folder on the host and uploads it to S3.
with DAG(
    dag_id="upload_folder_to_s3",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["storage", "s3", "upload"],
    params={
        "source_dir": Param(
            default="",
            type="string",
            description="Host folder to compress and upload (defaults to CITYJSON_STORAGE_DIR).",
        ),
        "s3_prefix": Param(
            default="",
            type="string",
            description="Optional S3 key prefix (e.g. a run id or subfolder).",
        ),
        "zip_name": Param(
            default="",
            type="string",
            description="Filename to use for the zip archive (defaults to <run_id>.zip).",
        ),
    },
) as dag:
    upload_task = PythonOperator(
        task_id="zip_and_upload",
        python_callable=_upload_folder_callable,
        provide_context=True,
    )
