from airflow.sdk import DAG, Param
from airflow.sdk.definitions.param import ParamsDict
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import os
import zipfile


# This method downloads a file from S3 to the host.
def extract_zip(src_dir: str, zip_name: str, dest_dir: str):
    zip_path = os.path.join(src_dir, zip_name)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(dest_dir)


DAG_ID = "extract_zip"

# This is a DAG which extracts a zip file.
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["Zip", "Extract"],
    params=ParamsDict({
        "src_dir": Param(
            default="",
            type="string",
            description="Name of the Directory where the Zip is that needs to be extracted",
        ),
        "dest_dir": Param(
            default="",
            type="string",
            description="Name of the Directory where the extracted files will be",
        ),
        "zip_name": Param(
            default="",
            type="string",
            description="Name of the Zip-file to extract",
        ),
    }),
) as dag:

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

    download = PythonOperator(
        task_id="extract_zip",
        python_callable=extract_zip_callable,
        provide_context=True,
    )