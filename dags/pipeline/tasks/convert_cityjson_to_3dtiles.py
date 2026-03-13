from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import BaseOperator
from docker.types import Mount
import os
from pipeline.config import JSON_TO_3D_TILES_IMAGE, DOCKER_HOST, WORK_DIR

container_env = {
    "APPEARANCE": os.getenv("CITYJSON_APPEARANCE", "rgbTexture"),
    "THREAD_COUNT": os.getenv("CITYJSON_THREAD_COUNT", "4"),
    "HAS_ALPHA_ENABLED": os.getenv("CITYJSON_HAS_ALPHA_ENABLED", "true"),
    "SIMPLIFY_ADDRESSES": os.getenv("CITYJSON_SIMPLIFY_ADDRESSES", "false"),
    "SHOW_STACK_TRACE": os.getenv("CITYJSON_SHOW_STACK_TRACE", "false"),
}

def make_convert_cityjson_to_3dtiles_task(fromDir: str, toDir: str) -> BaseOperator:
    return DockerOperator(
        task_id="generate_tiles",
        image=JSON_TO_3D_TILES_IMAGE,
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=WORK_DIR,
                target="/work",
                type="bind",
            ),
        ],
        environment={
            **container_env,
            "SRC_SRS": "{{ params.source_crs if params.source_crs is not none else '' }}",
            "INPUT_DIR": f"/work/{fromDir}",
            "OUTPUT_DIR": f"/work/{toDir}",
            "SKIP_CONVERSION": "true"
        },
        user=f"{os.getuid()}:{os.getgid()}",
        docker_url=DOCKER_HOST,
    )
