from airflow.providers.standard.operators.python import PythonOperator
import os
import zipfile
from pipeline.config import WORK_DIR


def extract_zip(zip_path: str, dest: str):
    if not os.path.isfile(zip_path):
        raise FileNotFoundError(f"Zip file not found: {zip_path}")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(dest)


def _extract_zip_callable(params):
    key = params.get("key")
    if not key:
        raise ValueError("Missing param: key")
    extract_zip(
        zip_path=os.path.join(WORK_DIR, "zip", key),
        dest=os.path.join(WORK_DIR, "gml_in"),
    )


def make_extract_zip_task() -> PythonOperator:
    return PythonOperator(
        task_id="extract_zip",
        python_callable=_extract_zip_callable,
    )
