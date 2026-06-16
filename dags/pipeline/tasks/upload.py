import os
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pipeline.config import get_job_dir
from pipeline import manifest as mf


def _clear_bucket(bucket: str):
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


def _clear_bucket_callable(params, run_id, task_id, bucket_param):
    job_dir = get_job_dir(run_id)
    bucket = params.get(bucket_param)
    if not bucket:
        raise ValueError(f"Missing param: {bucket_param}")
    mf.update_step(job_dir, task_id, "running")
    try:
        _clear_bucket(bucket)
        mf.update_step(job_dir, task_id, "success")
    except Exception as e:
        mf.update_step(job_dir, task_id, "failed", error=str(e))
        raise


def make_clear_bucket_task(task_id: str, bucket_param: str) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=_clear_bucket_callable,
        op_kwargs={"task_id": task_id, "bucket_param": bucket_param},
    )


def _upload_folder(src_dir: str, bucket: str):
    if not os.path.isdir(src_dir):
        raise FileNotFoundError(f"Source directory does not exist: {src_dir}")
    hook = S3Hook(aws_conn_id=None)
    for root, _dirs, files in os.walk(src_dir):
        for fname in files:
            full_path = os.path.normpath(os.path.join(root, fname))
            key = os.path.relpath(full_path, src_dir)
            hook.load_file(filename=full_path, key=key, bucket_name=bucket, replace=True)
            print(f"Uploaded '{full_path}' to s3://{bucket}/{key}")


def _upload_callable(params, run_id, task_id, src_dir, bucket_param):
    job_dir = get_job_dir(run_id)
    bucket = params.get(bucket_param)
    if not bucket:
        raise ValueError(f"Missing param: {bucket_param}")
    mf.update_step(job_dir, task_id, "running")
    try:
        _upload_folder(os.path.join(job_dir, src_dir), bucket)
        mf.update_step(job_dir, task_id, "success")
    except Exception as e:
        mf.update_step(job_dir, task_id, "failed", error=str(e))
        raise


def make_upload_task(task_id: str, src_dir: str, bucket_param: str) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=_upload_callable,
        op_kwargs={"task_id": task_id, "src_dir": src_dir, "bucket_param": bucket_param},
    )
