from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os

def download_from_s3(bucket: str, key: str, dest: str):
    hook = S3Hook(aws_conn_id="aws_default")
    obj = hook.get_key(key, bucket_name=bucket)
    if obj is None:
        raise FileNotFoundError(f"s3://{bucket}/{key} not found")
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    obj.download_file(dest)


def download_task_callable(bucket: str, key: str, dest_dir: str, target_filename: str):
    dest_path = os.path.join(dest_dir, target_filename)
    download_from_s3(
        bucket,
        key,
        dest_path,
    )


def make_download_task(bucket: str, key: str, dest_dir: str, target_filename: str) -> PythonOperator:
    return PythonOperator(
        task_id="download_file_task",
        python_callable=download_task_callable,
        op_kwargs={
            'bucket': bucket,
            'key': key,
            'dest_dir': dest_dir,
            'target_filename': target_filename,
        },
    )