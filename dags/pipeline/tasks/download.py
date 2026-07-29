import os
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pipeline.config import get_job_dir
from pipeline import manifest as mf


def download_from_s3(bucket: str, key: str, dest: str):
    hook = S3Hook(aws_conn_id=None)
    obj = hook.get_key(key, bucket_name=bucket)
    if obj is None:
        raise FileNotFoundError(f"s3://{bucket}/{key} not found")
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    obj.download_file(dest)


def _download_callable(params, run_id, task_id):
    job_dir = get_job_dir(run_id)
    bucket = params.get("bucket")
    key = params.get("key")
    if not bucket:
        raise ValueError("Missing param: bucket")
    if not key:
        raise ValueError("Missing param: key")
    mf.update_step(job_dir, task_id, "running")
    try:
        download_from_s3(bucket, key, os.path.join(job_dir, "zip", key))
        mf.update_step(job_dir, task_id, "success")
    except Exception as e:
        mf.update_step(job_dir, task_id, "failed", error=str(e))
        raise


def make_download_task() -> PythonOperator:
    return PythonOperator(
        task_id="download_file_task",
        python_callable=_download_callable,
        op_kwargs={"task_id": "download_file_task"},
    )


def _download_gpkg_callable(params, run_id, task_id):
    job_dir = get_job_dir(run_id)
    sources = [
        ("age_zones_key", "age zones", "age_zones.gpkg"),
        ("geothermal_key", "geothermal data", "geothermal.gpkg"),
    ]
    downloads = [
        (label, key, filename)
        for param_name, label, filename in sources
        if (key := params.get(param_name))
    ]
    if not downloads:
        print("Skipping GeoPackage downloads: no enrichment data keys set")
        mf.update_step(job_dir, task_id, "skipped")
        return
    bucket = params.get("bucket")
    if not bucket:
        raise ValueError("Missing param: bucket")
    mf.update_step(job_dir, task_id, "running")
    try:
        for label, key, filename in downloads:
            print(f"Downloading {label} from s3://{bucket}/{key}")
            download_from_s3(bucket, key, os.path.join(job_dir, "gpkg", filename))
        mf.update_step(job_dir, task_id, "success")
    except Exception as e:
        mf.update_step(job_dir, task_id, "failed", error=str(e))
        raise


def make_download_gpkg_task() -> PythonOperator:
    return PythonOperator(
        task_id="download_gpkg_task",
        python_callable=_download_gpkg_callable,
        op_kwargs={"task_id": "download_gpkg_task"},
    )
