import os
from airflow.models import BaseOperator
from pipeline.config import ENRICH_IMAGE, DOCKER_HOST
from pipeline.operators.job_docker_operator import JobDockerOperator


def make_enrich_cityjson_task(
    src_dir: str,
    out_dir: str,
    address_db_dir: str,
    with_age_zones: bool = False,
    with_geothermal: bool = False,
) -> BaseOperator:
    environment = {
        "SOURCE_CRS_FALLBACK": "{{ params.source_crs if params.source_crs is not none else '' }}",
        "MUNICIPALITY_KEY": "{{ params.municipality_key }}",
        "ADJACENCY": "1",
        "INPUT_DIR": f"/work/{src_dir}",
        "OUTPUT_DIR": f"/work/{out_dir}",
        "ADDRESS_OUTPUT": f"/work/{address_db_dir}/det-rg-addresses.sqlite",
    }
    if with_age_zones:
        environment["AGE_ZONES_FILE"] = "{{ '/work/gpkg/age_zones.gpkg' if params.get('age_zones_key') else '' }}"
    if with_geothermal:
        environment["GEOTHERMAL_FILE"] = "{{ '/work/gpkg/geothermal.gpkg' if params.get('geothermal_key') else '' }}"

    return JobDockerOperator(
        task_id="enrich_cityjson",
        image=ENRICH_IMAGE,
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        environment=environment,
        command="node dist/cli.mjs",
        user=f"{os.getuid()}:{os.getgid()}",
        docker_url=DOCKER_HOST,
    )
