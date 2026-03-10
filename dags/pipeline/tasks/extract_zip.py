from airflow.providers.standard.operators.python import PythonOperator
import os
import zipfile

def extract_zip_callable(src_dir: str, dest_dir: str, zip_name: str):
    zip_path = os.path.join(src_dir, zip_name)
    if not os.path.isfile(zip_path):
        raise FileNotFoundError(f"Zip file not found: {zip_path}")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(dest_dir)

def make_extract_zip_task(src_dir: str, dest_dir: str, zip_name: str) -> PythonOperator:
    return PythonOperator(
        task_id="extract_zip",
        python_callable=extract_zip_callable,
        op_kwargs={
            'src_dir': src_dir,
            'dest_dir': dest_dir,
            'zip_name': zip_name,
        },
    )