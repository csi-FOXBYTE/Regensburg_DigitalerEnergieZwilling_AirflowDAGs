from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import BaseOperator
from docker.types import Mount
import os
from pipeline.config import ENRICH_IMAGE, DOCKER_HOST, WORK_DIR


def make_enrich_cityjson_task(src_dir: str, address_db_dir: str, with_age_zones: bool = False) -> BaseOperator:
    mounts = [
        Mount(source=f"{WORK_DIR}/{src_dir}", target="/data", type="bind"),
        Mount(source=f"{WORK_DIR}/{address_db_dir}", target="/address_db", type="bind"),
    ]
    if with_age_zones:
        mounts.append(Mount(source=f"{WORK_DIR}/gpkg", target="/gpkg_in", type="bind", read_only=True))

    environment = {
        "SOURCE_CRS_FALLBACK": "{{ params.source_crs if params.source_crs is not none else '' }}",
        "ADJACENCY": "1",
        "ADDRESS_OUTPUT": "/address_db/det-rg-addresses.sqlite",
    }
    if with_age_zones:
        environment["AGE_ZONES_FILE"] = "{{ '/gpkg_in/age_zones.gpkg' if params.get('age_zones_key') else '' }}"

    return DockerOperator(
        task_id="enrich_cityjson",
        image=ENRICH_IMAGE,
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        environment=environment,
        mounts=mounts,
        user=f"{os.getuid()}:{os.getgid()}",
        docker_url=DOCKER_HOST,
    )
