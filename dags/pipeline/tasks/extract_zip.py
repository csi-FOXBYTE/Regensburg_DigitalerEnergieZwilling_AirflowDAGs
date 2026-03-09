from airflow.providers.standard.operators.python import PythonOperator
import os
import zipfile

# This method extracts the zip.
def extractZip(src_dir: str, zip_name: str, dest_dir: str):
    zip_path = os.path.join(src_dir, zip_name)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(dest_dir)

def extract_zip_callable(**context):
    params = context["params"]
    src_dir = params.get("src_dir")
    zip_name = params.get("zip_name")
    dest_dir = params.get("dest_dir")
    extract_zip(
        src_dir=src_dir,
        zip_name=zip_name,
        dest_dir=dest_dir
    )

def extract_zip() -> PythonOperator:
    return PythonOperator(
        task_id="extract_zip",
        python_callable=extract_zip_callable,
        provide_context=True,
    )
