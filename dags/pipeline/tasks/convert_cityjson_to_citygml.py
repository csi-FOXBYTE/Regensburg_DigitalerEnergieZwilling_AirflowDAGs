import os
from airflow.models import BaseOperator
from pipeline.config import GML_TOOLS_IMAGE, DOCKER_HOST
from pipeline.operators.job_docker_operator import JobDockerOperator


def make_convert_cityjson_to_citygml_task(from_dir: str, to_dir: str) -> BaseOperator:
    return JobDockerOperator(
        task_id="convert_cityjson_to_citygml",
        image=GML_TOOLS_IMAGE,
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        user=f"{os.getuid()}:{os.getgid()}",
        docker_url=DOCKER_HOST,
        command=f"from-cityjson /work/{from_dir} --output /work/{to_dir}",
    )
