from airflow.providers.standard.operators.python import PythonOperator
import os
from s3 import _build_s3_client


def upload_folder(src_dir: str, bucket: str):
    if not os.path.isdir(src_dir):
        raise FileNotFoundError(f"Source directory does not exist: {src_dir}")
    client = _build_s3_client()
    for root, _dirs, files in os.walk(src_dir):
        for fname in files:
            full_path = os.path.normpath(os.path.join(root, fname))
            key = os.path.relpath(full_path, src_dir)
            client.upload_file(full_path, bucket, key)
            print(f"Uploaded '{full_path}' to s3://{bucket}/{key}")


def _upload_callable(params, src_dir, bucket_param):
    bucket = params.get(bucket_param)
    if not bucket:
        raise ValueError(f"Missing param: {bucket_param}")
    upload_folder(src_dir, bucket)


def make_upload_task(task_id: str, src_dir: str, bucket_param: str) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=_upload_callable,
        op_kwargs={"src_dir": src_dir, "bucket_param": bucket_param},
    )
