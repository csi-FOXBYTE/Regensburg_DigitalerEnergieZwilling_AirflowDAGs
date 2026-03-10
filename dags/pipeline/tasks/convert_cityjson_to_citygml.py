from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import BaseOperator
from docker.types import Mount
from pipeline.config import *


def makeConvertCityJSONToCityGMLTask(fromDir: str, toDir: str) -> BaseOperator:
  return DockerOperator(
    task_id="convert_cityjson_to_citygml",
    image=GML_TOOLS_IMAGE,
    api_version="auto",
    auto_remove="success",
    mount_tmp_dir=False,
    mounts=[
      Mount(
        source=WORK_DIR,
        target="/work",
        type="bind"
      )
    ],
    docker_url=DOCKER_HOST,
    command=f"to-cityjson /data/{fromDir} --output /data/{toDir}"
  )