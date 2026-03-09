from airflow.providers.standard.operators.python import PythonOperator
import os
from s3 import _build_s3_client

# host directories that are visible inside the scheduler/container
STORAGE_DIR_ON_HOST = os.getenv("CITYJSON_STORAGE_DIR", "/opt/airflow/data/storage")

# S3 configuration (match values used in s3-gui and .env.example)
S3_BUCKET = os.getenv("S3_BUCKET", "external-downloads")


def _upload_folder_callable(**context):
    """Airflow callable that uploads files to S3.

    Parameters are supplied via ``dag_run.conf`` or the UI:

    * ``source_dir`` (optional) – path on the host to compress; defaults to
      ``CITYJSON_STORAGE_DIR``.
    * ``s3_prefix`` (optional) – prefix to prepend to the S3 key.
    """
    params = context["params"]
    source_dir = params.get("source_dir") or STORAGE_DIR_ON_HOST
    if not os.path.isdir(source_dir):
        raise FileNotFoundError(f"Source directory does not exist: {source_dir}")

    s3_prefix = (params.get("s3_prefix") or "").strip().strip("/")
    client = _build_s3_client()

   # Walk through the directory and upload each file
    for root, _dirs, files in os.walk(source_dir):
        for fname in files:
            full_path = os.path.normpath(os.path.join(root, fname))
            relative_path = os.path.relpath(full_path, source_dir)
            key = relative_path if not s3_prefix else f"{s3_prefix}/{relative_path}"
            client.upload_file(full_path, S3_BUCKET, key)
            print(f"Uploaded '{full_path}' to s3://{S3_BUCKET}/{key}")

def upload() -> PythonOperator:
    return PythonOperator(
        task_id="upload_to_s3",
        python_callable=_upload_folder_callable,
        provide_context=True,
    )
