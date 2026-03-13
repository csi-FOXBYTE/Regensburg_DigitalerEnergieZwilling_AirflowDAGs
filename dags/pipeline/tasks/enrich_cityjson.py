from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import BaseOperator
from docker.types import Mount
import os
from pipeline.config import ENRICH_IMAGE, DOCKER_HOST, WORK_DIR


SRC_SRS = os.getenv("CITYJSON_SRC_SRS", "")

container_env = {}

if SRC_SRS:
    container_env["SOURCE_CRS_FALLBACK"] = SRC_SRS


def make_enrich_cityjson_task(dir: str) -> BaseOperator:
    return DockerOperator(
        task_id="enrich_cityjson",
        image=ENRICH_IMAGE,
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        environment={
            "SOURCE_CRS_FALLBACK": "{{ params.source_crs if params.source_crs is not none else '' }}"
        },
        mounts=[
            Mount(
                source=f"{WORK_DIR}/{dir}",
                target="/data",
                type="bind",
            )
        ],
        user=f"{os.getuid()}:{os.getgid()}",
        docker_url=DOCKER_HOST,
    )
