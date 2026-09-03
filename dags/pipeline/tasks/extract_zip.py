import os
import zipfile
from airflow.providers.standard.operators.python import PythonOperator
from pipeline.config import INPUT_ZIP_FILENAME, get_job_dir
from pipeline import manifest as mf


def _extract_zip_callable(params, run_id, task_id):
    job_dir = get_job_dir(run_id)
    if not params.get("key"):
        raise ValueError("Missing param: key")
    mf.update_step(job_dir, task_id, "running")
    try:
        zip_path = os.path.join(job_dir, "zip", INPUT_ZIP_FILENAME)
        if not os.path.isfile(zip_path):
            raise FileNotFoundError(f"Zip file not found: {zip_path}")
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(os.path.join(job_dir, "gml_in"))
        mf.update_step(job_dir, task_id, "success")
    except Exception as e:
        mf.update_step(job_dir, task_id, "failed", error=str(e))
        raise


def make_extract_zip_task() -> PythonOperator:
    return PythonOperator(
        task_id="extract_zip",
        python_callable=_extract_zip_callable,
        op_kwargs={"task_id": "extract_zip"},
    )
