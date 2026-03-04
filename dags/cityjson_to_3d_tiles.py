from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


DAG_ID = "cityjson_to_3d_tiles"
IMAGE = "ghcr.io/csi-foxbyte/cityjson-to-3d-tiles:latest"
WORK_DIR_ON_HOST = os.getenv(
    "CITYJSON_WORK_DIR",
    "/opt/airflow/data/cityjson-to-3d-tiles",
)
SRC_SRS = os.getenv("CITYJSON_SRC_SRS", "")
DEST_SRS = os.getenv("CITYJSON_DEST_SRS", "")


container_env = {
    "APPEARANCE": os.getenv("CITYJSON_APPEARANCE", "rgbTexture"),
    "THREAD_COUNT": os.getenv("CITYJSON_THREAD_COUNT", "4"),
    "HAS_ALPHA_ENABLED": os.getenv("CITYJSON_HAS_ALPHA_ENABLED", "true"),
    "SIMPLIFY_ADDRESSES": os.getenv("CITYJSON_SIMPLIFY_ADDRESSES", "false"),
    "SHOW_STACK_TRACE": os.getenv("CITYJSON_SHOW_STACK_TRACE", "false"),
}

if SRC_SRS:
    container_env["SRC_SRS"] = SRC_SRS
if DEST_SRS:
    container_env["DEST_SRS"] = DEST_SRS


def _normalize_docker_host_path(path: str) -> str:
    """Normalize Windows paths for Linux Docker daemon inside Airflow containers."""
    if len(path) >= 3 and path[1] == ":" and path[2] in ("/", "\\"):
        drive = path[0].lower()
        rest = path[3:].replace("\\", "/")
        return f"/mnt/{drive}/{rest}"
    return path


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["3d-tiles", "cityjson", "citygml"],
) as dag:
    DockerOperator(
        task_id="generate_tiles",
        image=IMAGE,
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        # Host folder must contain input files and will receive /work/cityjson and /work/tiles.
        mounts=[
            Mount(
                source=_normalize_docker_host_path(WORK_DIR_ON_HOST),
                target="/work",
                type="bind",
            )
        ],
        environment=container_env,
        docker_url=os.getenv("DOCKER_HOST", "unix://var/run/docker.sock"),
    )
