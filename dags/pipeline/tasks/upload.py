from airflow.providers.standard.operators.python import PythonOperator
import os
from pipeline.config import WORK_DIR
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def clear_bucket(bucket: str):
    hook = S3Hook(aws_conn_id=None)
    s3_client = hook.get_conn()
    paginator = s3_client.get_paginator("list_objects_v2")
    total_deleted = 0
    for page in paginator.paginate(Bucket=bucket, PaginationConfig={"PageSize": 100}):
        objects = page.get("Contents", [])
        if objects:
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
            )
            total_deleted += len(objects)
    print(f"Cleared {total_deleted} existing object(s) from s3://{bucket}/")


def _clear_bucket_callable(params, bucket_param):
    bucket = params.get(bucket_param)
    if not bucket:
        raise ValueError(f"Missing param: {bucket_param}")
    clear_bucket(bucket)


def make_clear_bucket_task(task_id: str, bucket_param: str) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=_clear_bucket_callable,
        op_kwargs={"bucket_param": bucket_param},
    )


def upload_folder(src_dir: str, bucket: str):
    if not os.path.isdir(src_dir):
        raise FileNotFoundError(f"Source directory does not exist: {src_dir}")
    hook = S3Hook(aws_conn_id=None)
    for root, _dirs, files in os.walk(src_dir):
        for fname in files:
            full_path = os.path.normpath(os.path.join(root, fname))
            key = os.path.relpath(full_path, src_dir)

            hook.load_file(
                filename=full_path,
                key=key,
                bucket_name=bucket,
                replace=True,
            )
            print(f"Uploaded '{full_path}' to s3://{bucket}/{key}")


def _upload_callable(params, src_dir, bucket_param):
    bucket = params.get(bucket_param)
    if not bucket:
        raise ValueError(f"Missing param: {bucket_param}")
    src = os.path.join(WORK_DIR, src_dir)
    upload_folder(src, bucket)


def make_upload_task(task_id: str, src_dir: str, bucket_param: str) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=_upload_callable,
        op_kwargs={"src_dir": src_dir, "bucket_param": bucket_param},
    )
