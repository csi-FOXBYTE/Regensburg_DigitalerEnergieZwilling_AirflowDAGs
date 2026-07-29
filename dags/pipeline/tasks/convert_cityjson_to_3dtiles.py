import os
from airflow.models import BaseOperator
from pipeline.config import JSON_TO_3D_TILES_IMAGE, DOCKER_HOST
from pipeline.operators.job_docker_operator import JobDockerOperator


_container_env = {
    "APPEARANCE": os.getenv("CITYJSON_APPEARANCE", "rgbTexture"),
    "THREAD_COUNT": os.getenv("CITYJSON_THREAD_COUNT", "4"),
    "HAS_ALPHA_ENABLED": os.getenv("CITYJSON_HAS_ALPHA_ENABLED", "true"),
    "SIMPLIFY_ADDRESSES": os.getenv("CITYJSON_SIMPLIFY_ADDRESSES", "false"),
    "SEMANTIC_SURFACE_COLORS": os.getenv(
        "CITYJSON_SEMANTIC_SURFACE_COLORS",
        '{"RoofSurface":"#e30613"}',
    ),
    "SHOW_STACK_TRACE": os.getenv("CITYJSON_SHOW_STACK_TRACE", "false"),
}


def make_convert_cityjson_to_3dtiles_task(from_dir: str, to_dir: str) -> BaseOperator:
    return JobDockerOperator(
        task_id="generate_tiles",
        image=JSON_TO_3D_TILES_IMAGE,
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        environment={
            **_container_env,
            "SRC_SRS": "{{ params.source_crs if params.source_crs is not none else '' }}",
            "INPUT_DIR": f"/work/{from_dir}",
            "OUTPUT_DIR": f"/work/{to_dir}",
            "SKIP_CONVERSION": "true",
        },
        user=f"{os.getuid()}:{os.getgid()}",
        docker_url=DOCKER_HOST,
    )
