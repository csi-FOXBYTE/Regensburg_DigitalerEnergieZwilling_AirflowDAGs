from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.definitions.param import ParamsDict
import os
import zipfile

# This callable extracts the zip.
def extract_zip_callable(params):
    required_params = ["src_dir", "zip_name", "dest_dir"]
    for rq in required_params:
        if not params.get(rq):
            raise ValueError(f"Missing parameter: {rq}")

    src_dir = params.get("src_dir")
    zip_name = params.get("zip_name")
    dest_dir = params.get("dest_dir")
    zip_path = os.path.join(src_dir, zip_name)
    if not os.path.isfile(zip_path):
        raise FileNotFoundError(f"Zip file not found: {zip_path}")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(dest_dir)

def extract_zip(params: ParamsDict) -> PythonOperator:
    return PythonOperator(
        task_id="extract_zip",
        python_callable=extract_zip_callable,
        op_kwargs={'params': params}
    )

"""
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
"""