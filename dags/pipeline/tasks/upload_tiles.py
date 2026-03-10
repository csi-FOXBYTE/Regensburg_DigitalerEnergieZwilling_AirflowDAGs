from airflow.providers.standard.operators.python import PythonOperator
import os
from s3 import _build_s3_client

def upload_folder_callable(tiles_dir: str, tiles_bucket: str, s3_prefix: str):
    if not os.path.isdir(tiles_dir):
        raise FileNotFoundError(f"Source directory does not exist: {tiles_dir}")
    s3_prefix = (s3_prefix or "").strip().strip("/")
    client = _build_s3_client()
    for root, _dirs, files in os.walk(tiles_dir):
        for fname in files:
            full_path = os.path.normpath(os.path.join(root, fname))
            relative_path = os.path.relpath(full_path, tiles_dir)
            key = relative_path if not s3_prefix else f"{s3_prefix}/{relative_path}"
            client.upload_file(full_path, tiles_bucket, key)
            print(f"Uploaded '{full_path}' to s3://{tiles_bucket}/{key}")

def make_tiles_upload_task(tiles_dir: str, tiles_bucket: str, s3_prefix: str) -> PythonOperator:
    return PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_folder_callable,
        op_kwargs={
            'tiles_dir': tiles_dir,
            'tiles_bucket': tiles_bucket,
            's3_prefix': s3_prefix,
        },
    )