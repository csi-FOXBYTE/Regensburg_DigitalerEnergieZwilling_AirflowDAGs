from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.definitions.param import ParamsDict
from airflow.sdk import Param
import os
from s3 import _build_s3_client

# host directories that are visible inside the scheduler/container
# STORAGE_DIR_ON_HOST = os.getenv("CITYJSON_STORAGE_DIR", "/opt/airflow/data/storage")

# S3 configuration (match values used in s3-gui and .env.example)
S3_BUCKET = os.getenv("S3_BUCKET", "external-downloads")


def _upload_folder_callable(params):
    """Airflow callable that uploads files to S3.

    Parameters are supplied via ``dag_run.conf`` or the UI:

    * ``source_dir`` (optional) – path on the host to compress; defaults to
      ``CITYJSON_STORAGE_DIR``.
    * ``s3_prefix`` (optional) – prefix to prepend to the S3 key.
    """

    tilesdir = params.get("tiles_dir")
    citygmldir = params.get("citygml_dir")

    tilesbucket = params.get("tiles_bucket")
    citygmlbucket = params.get("citygml_bucket")

#  source_dir = params.get("source_dir") or STORAGE_DIR_ON_HOST
    if not os.path.isdir(tilesdir):
        raise FileNotFoundError(f"Source directory does not exist: {tilesdir}")
    
    if not os.path.isdir(citygmldir):
        raise FileNotFoundError(f"Source directory does not exist: {citygmldir}")

    s3_prefix = (params.get("s3_prefix") or "").strip().strip("/")
    client = _build_s3_client()

   # Walk through the tiles-directory and upload each file
    for root, _dirs, files in os.walk(tilesdir):
        for fname in files:
            full_path = os.path.normpath(os.path.join(root, fname))
            relative_path = os.path.relpath(full_path, tilesdir)
            key = relative_path if not s3_prefix else f"{s3_prefix}/{relative_path}"
            client.upload_file(full_path, tilesbucket, key)
            print(f"Uploaded '{full_path}' to s3://{tilesbucket}/{key}")

    # Walk through the gml-directory and upload each file
    for root, _dirs, files in os.walk(citygmldir):
        for fname in files:
            full_path = os.path.normpath(os.path.join(root, fname))
            relative_path = os.path.relpath(full_path, citygmldir)
            key = relative_path if not s3_prefix else f"{s3_prefix}/{relative_path}"
            client.upload_file(full_path, citygmlbucket, key)
            print(f"Uploaded '{full_path}' to s3://{citygmlbucket}/{key}")

def upload(params: ParamsDict) -> PythonOperator:
    return PythonOperator(
        task_id="upload_to_s3",
        python_callable=_upload_folder_callable,
        provide_context=True,
        op_kwargs={'params': params}
    )

"""
params=ParamsDict({
    "tiles_dir": Param(
        type="string",
        description="Host folder containing the 3d tiles",
    ),
    "citygml_dir": Param(
        type="string",
        description="Host folder containing the citygml files"
    ),
    "tiles_bucket": Param(
        type="string",
        description="Name of the bucket where the 3d tiles will be uploaded"
    ),
    "citygml_bucket": Param(
        type="string",
        description="Name of the bucket where the citygml files will be uploaded"
    ),
    "s3_prefix": Param(
        default="",
        type="string",
        description="Optional S3 key prefix (e.g. a run id or subfolder).",
    )
}),
"""