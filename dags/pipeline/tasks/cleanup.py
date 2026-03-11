from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
from pipeline.config import WORK_DIR


def _cleanup_callable():
    return

def make_cleanup_task(directories: list[str]) -> PythonOperator:
    return PythonOperator(
        task_id="download_file_task",
        python_callable=_cleanup_callable,
    )