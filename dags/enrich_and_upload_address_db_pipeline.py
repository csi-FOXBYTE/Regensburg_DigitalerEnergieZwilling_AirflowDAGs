from airflow.sdk import DAG, Param
from airflow.sdk.definitions.param import ParamsDict
from datetime import datetime
from pipeline.tasks.download import make_download_task
from pipeline.tasks.extract_zip import make_extract_zip_task
from pipeline.tasks.convert_citygml_to_cityjson import make_convert_citygml_to_cityjson_task
from pipeline.tasks.enrich_cityjson import make_enrich_cityjson_task
from pipeline.tasks.upload import make_upload_task
from pipeline.tasks.ensure_dirs import make_ensure_dirs_task
from pipeline.tasks.cleanup import make_cleanup_task

DAG_ID = "enrich_and_upload_address_db_pipeline"

DIRS = ["zip", "gml_in", "json", "address_db"]

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["S3", "Host", "Download"],
    params=ParamsDict({
        "bucket": Param(
            type="string",
            description="Name of the S3 bucket containing the zip file",
        ),
        "key": Param(
            type="string",
            description="Key (path) of the zip file in the bucket",
        ),
        "tiles_output_bucket": Param(
            type="string",
            description="S3 bucket to upload the address database to",
        ),
        "source_crs": Param(
            default="+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs",
            type="string",
            description="Source Coordinate System",
        ),
    }),
) as dag:
    ensure_dirs_task = make_ensure_dirs_task(DIRS)
    download_task = make_download_task()
    extract_task = make_extract_zip_task()
    gml_to_json_task = make_convert_citygml_to_cityjson_task("gml_in", "json")
    enrich_task = make_enrich_cityjson_task("json", "address_db")
    upload_address_db_task = make_upload_task("upload_address_db", "address_db", "tiles_output_bucket")
    cleanup_task = make_cleanup_task(DIRS)

    ensure_dirs_task >> download_task >> extract_task >> gml_to_json_task >> enrich_task
    enrich_task >> upload_address_db_task >> cleanup_task
