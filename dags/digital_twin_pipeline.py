from airflow.sdk import DAG, Param
from airflow.sdk.definitions.param import ParamsDict
from datetime import datetime
from pipeline.tasks.download import make_download_task
from pipeline.tasks.extract_zip import make_extract_zip_task
from pipeline.tasks.convert_citygml_to_cityjson import make_convert_citygml_to_cityjson_task
from pipeline.tasks.enrich_cityjson import make_enrich_cityjson_task
from pipeline.tasks.convert_cityjson_to_3dtiles import make_convert_cityjson_to_3dtiles_task
from pipeline.tasks.convert_cityjson_to_citygml import make_convert_cityjson_to_citygml_task
from pipeline.tasks.upload import make_upload_task
from pipeline.tasks.ensure_dirs import make_ensure_dirs_task
from pipeline.tasks.cleanup import make_cleanup_task

DAG_ID = "digital_twin_pipeline"

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
            description="Key (path) of the zip file in the bucket — also used as the local filename",
        ),
        "tiles_output_bucket": Param(
            type="string",
            description="S3 bucket to upload the 3D tiles output to",
        ),
        "gml_output_bucket": Param(
            type="string",
            description="S3 bucket to upload the CityGML output to",
        ),
        "source_crs": Param(
            default="+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs",
            type="string",
            description="Source Coordinate System"
        )
    }),
) as dag:
    ensure_dirs_task = make_ensure_dirs_task(["zip", "gml_in", "json", "gml_out", "3d_tiles"])
    download_task = make_download_task()
    extract_task = make_extract_zip_task()
    gml_to_json_task = make_convert_citygml_to_cityjson_task("gml_in", "json")
    enrich_task = make_enrich_cityjson_task("json")
    json_to_3d_task = make_convert_cityjson_to_3dtiles_task("json", "3d_tiles")
    json_to_gml_task = make_convert_cityjson_to_citygml_task("json", "gml_out")
    upload_tiles_task = make_upload_task("upload_tiles", "3d_tiles", "tiles_output_bucket")
    upload_gml_task = make_upload_task("upload_gml", "gml_out", "gml_output_bucket")
    cleanup_task = make_cleanup_task(["zip", "gml_in", "json", "gml_out", "3d_tiles"])

    ensure_dirs_task >> download_task >> extract_task >> gml_to_json_task >> enrich_task
    enrich_task >> json_to_3d_task >> upload_tiles_task
    enrich_task >> json_to_gml_task >> upload_gml_task
    [upload_tiles_task, upload_gml_task] >> cleanup_task
