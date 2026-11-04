from airflow.sdk import DAG, Param, TriggerRule
from airflow.sdk.definitions.param import ParamsDict
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from pipeline.tasks.preparation import make_preparation_task
from pipeline.tasks.download import make_download_task, make_download_gpkg_task
from pipeline.tasks.extract_zip import make_extract_zip_task
from pipeline.tasks.convert_citygml_to_cityjson import make_convert_citygml_to_cityjson_task
from pipeline.tasks.enrich_cityjson import make_enrich_cityjson_task
from pipeline.tasks.convert_cityjson_to_3dtiles import make_convert_cityjson_to_3dtiles_task
from pipeline.tasks.convert_cityjson_to_citygml import make_convert_cityjson_to_citygml_task
from pipeline.tasks.upload import make_publish_outputs_task
from pipeline.tasks.cleanup import make_cleanup_task
from pipeline.config import S3_CONN_ID, get_job_dir
from pipeline import manifest as mf

DAG_ID = "digital_twin_pipeline"

DIRS = ["zip", "gml_in", "json", "enriched_json", "gml_out", "3d_tiles", "gpkg", "address_db"]

STEP_NAMES = [
    "preparation",
    "download_file_task",
    "download_gpkg_task",
    "extract_zip",
    "convert_citygml_to_cityjson",
    "enrich_cityjson",
    "generate_tiles",
    "convert_cityjson_to_citygml",
    "publish_outputs",
    "cleanup",
    "finalize_manifest",
]


def _finalize_callable(run_id):
    mf.finalize_manifest(get_job_dir(run_id))


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    # Publication prunes stale objects (including staging data) from the
    # output buckets, so runs must not publish concurrently.
    max_active_runs=1,
    tags=["S3", "Host", "Download"],
    params=ParamsDict({
        "bucket": Param(
            type="string",
            description="Name of the S3 bucket containing the zip file",
        ),
        "key": Param(
            type="string",
            description="S3 object key of the zip file in the bucket",
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
            description="Source Coordinate System",
        ),
        "municipality_key": Param(
            default="09362000",
            type="string",
            description="Municipality key used to restrict the enrichment dataset to one city",
        ),
        "age_zones_key": Param(
            default=None,
            type=["string", "null"],
            description="Optional key of the Baualtersklassen GeoPackage in the input bucket",
        ),
        "geothermal_key": Param(
            default=None,
            type=["string", "null"],
            description="Optional key of the geothermal GeoPackage in the input bucket",
        ),
        "skip_cleanup": Param(
            default=False,
            type="boolean",
            description=(
                "Keep downloaded inputs and generated artifacts in the run "
                "workspace after successful uploads"
            ),
        ),
    }),
) as dag:
    preparation_task = make_preparation_task(DIRS, DAG_ID, STEP_NAMES)
    download_task = make_download_task(S3_CONN_ID)
    download_gpkg_task = make_download_gpkg_task(S3_CONN_ID)
    extract_task = make_extract_zip_task()
    gml_to_json_task = make_convert_citygml_to_cityjson_task("gml_in", "json")
    enrich_task = make_enrich_cityjson_task(
        "json",
        "enriched_json",
        "address_db",
        with_age_zones=True,
        with_geothermal=True,
    )
    json_to_3d_task = make_convert_cityjson_to_3dtiles_task("enriched_json", "3d_tiles")
    json_to_gml_task = make_convert_cityjson_to_citygml_task("enriched_json", "gml_out")
    # CityGML and 3D Tiles are staged and verified together; the tiles bucket
    # is promoted last so tileset.json switches only after everything else.
    publish_task = make_publish_outputs_task(
        "publish_outputs",
        [
            {"bucket_param": "gml_output_bucket", "directories": ["gml_out"]},
            {
                "bucket_param": "tiles_output_bucket",
                "directories": ["3d_tiles", "address_db"],
                "required_keys": ["det-rg-addresses.sqlite"],
                "entry_keys": ["tileset.json"],
            },
        ],
        S3_CONN_ID,
        exclusive_bucket_params=("bucket",),
    )
    cleanup_task = make_cleanup_task(DIRS, honor_skip_cleanup=True)
    finalize_task = PythonOperator(
        task_id="finalize_manifest",
        python_callable=_finalize_callable,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    preparation_task >> [download_task, download_gpkg_task]
    download_task >> extract_task >> gml_to_json_task >> enrich_task
    download_gpkg_task >> enrich_task
    enrich_task >> [json_to_3d_task, json_to_gml_task] >> publish_task
    publish_task >> cleanup_task >> finalize_task
