from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import BaseOperator
from docker.types import Mount
from pipeline.config import *


def make_convert_citygml_to_cityjson_task(fromDir: str, toDir: str) -> BaseOperator:
    return DockerOperator(
        task_id="convert_citygml_to_cityjson",
        image=GML_TOOLS_IMAGE,
        api_version="auto",
        auto_remove="never",
        mount_tmp_dir=False,
        retrieve_output=True,
        mounts=[
            Mount(
                source="airflow-data",
                target="/work",
                type="volume",
            )
        ],
        docker_url=DOCKER_HOST,
        command=f"to-cityjson /work/{fromDir}/*.gml --output /work/{toDir}"
    )
  