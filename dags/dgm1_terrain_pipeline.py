from datetime import datetime

from airflow.sdk import DAG, Param, TriggerRule
from airflow.sdk.definitions.param import ParamsDict
from airflow.providers.standard.operators.python import PythonOperator

from pipeline import manifest as mf
from pipeline.config import get_job_dir
from pipeline.tasks.cleanup import make_cleanup_task
from pipeline.tasks.preparation import make_preparation_task
from pipeline.tasks.terrain_processing import (
    make_build_vrt_task,
    make_generate_terrain_task,
    make_normalize_and_prepare_layer_task,
    make_validate_terrain_task,
)
from pipeline.tasks.terrain_publish import make_publish_terrain_task
from pipeline.tasks.terrain_sources import (
    make_download_sources_task,
    make_inspect_sources_task,
    make_parse_metalink_task,
    make_validate_source_reports_task,
)


DAG_ID = "dgm1_terrain_pipeline"

DIRS = [
    "dgm1_sources",
    "dgm1_metadata",
    "dgm1_vrt",
    "terrain",
    "terrain_validation",
]

STEP_NAMES = [
    "preparation",
    "parse_metalink",
    "download_sources",
    "inspect_sources",
    "validate_source_reports",
    "build_vrt",
    "generate_terrain",
    "normalize_and_prepare_layer",
    "validate_terrain",
    "publish_terrain",
    "cleanup",
    "finalize_manifest",
]


def _finalize_callable(run_id):
    mf.finalize_manifest(get_job_dir(run_id))


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["DGM1", "Cesium", "Terrain", "S3"],
    params=ParamsDict(
        {
            "terrain_output_bucket": Param(
                type="string",
                minLength=1,
                description=(
                    "Dedicated S3 bucket to clear and replace with the validated "
                    "Quantized Mesh terrain tileset"
                ),
            ),
            "skip_cleanup": Param(
                default=False,
                type="boolean",
                description=(
                    "Keep downloaded TIFFs, VRT, generated terrain, and validation "
                    "reports in the run workspace after successful publication"
                ),
            ),
        }
    ),
) as dag:
    preparation_task = make_preparation_task(DIRS, DAG_ID, STEP_NAMES)
    parse_metalink_task = make_parse_metalink_task()
    download_sources_task = make_download_sources_task()
    inspect_sources_task = make_inspect_sources_task()
    validate_source_reports_task = make_validate_source_reports_task()
    build_vrt_task = make_build_vrt_task()
    generate_terrain_task = make_generate_terrain_task()
    normalize_and_prepare_layer_task = make_normalize_and_prepare_layer_task()
    validate_terrain_task = make_validate_terrain_task()
    publish_terrain_task = make_publish_terrain_task()
    cleanup_task = make_cleanup_task(DIRS, honor_skip_cleanup=True)
    finalize_task = PythonOperator(
        task_id="finalize_manifest",
        python_callable=_finalize_callable,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        preparation_task
        >> parse_metalink_task
        >> download_sources_task
        >> inspect_sources_task
        >> validate_source_reports_task
        >> build_vrt_task
        >> generate_terrain_task
        >> normalize_and_prepare_layer_task
        >> validate_terrain_task
        >> publish_terrain_task
        >> cleanup_task
        >> finalize_task
    )
