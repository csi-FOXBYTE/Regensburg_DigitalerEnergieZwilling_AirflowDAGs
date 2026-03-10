from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import BaseOperator
from docker.types import Mount
from pipeline.config import *

SRC_SRS = os.getenv("CITYJSON_SRC_SRS", "")


container_env = {}

if SRC_SRS:
  container_env["SOURCE_CRS_FALLBACK"] = SRC_SRS

def makeEnrichCityJSONTask(dir: str) -> BaseOperator:
  return DockerOperator(
    task_id="convert_citygml_to_cityjson",
    image=ENRICH_IMAGE,
    api_version="auto",
    auto_remove="success",
    mount_tmp_dir=False,
    environment=container_env,
    mounts=[
      Mount(
        source=f"{WORK_DIR}/{dir}",
        target="/data",
        type="bind"
      )
    ],
    docker_url=DOCKER_HOST,
  )