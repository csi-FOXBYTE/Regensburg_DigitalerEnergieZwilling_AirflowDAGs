import json
from pathlib import Path

from airflow.providers.standard.operators.python import PythonOperator

from pipeline import manifest as mf
from pipeline import publication
from pipeline.config import (
    DGM1_UPLOAD_WORKERS,
    get_job_dir,
    sanitize_job_id,
    validate_bucket,
)
from pipeline.s3_connection import get_s3_hook
from pipeline.terrain_validation import TERRAIN_MEDIA_TYPE


LAYER_KEY = "layer.json"


def _publish_terrain_callable(params, run_id, task_id, aws_conn_id):
    job_dir = Path(get_job_dir(run_id))
    mf.update_step(str(job_dir), task_id, "running")
    try:
        bucket = params.get("terrain_output_bucket")
        if not bucket:
            raise ValueError("Missing param: terrain_output_bucket")
        validate_bucket(bucket)
        terrain_dir = job_dir / "terrain"
        layer_path = terrain_dir / LAYER_KEY
        if not layer_path.is_file() or not any(terrain_dir.rglob("*.terrain")):
            raise FileNotFoundError("Validated terrain output is incomplete")

        plan = publication.build_plan(
            bucket,
            sanitize_job_id(run_id),
            [terrain_dir],
            entry_keys=(LAYER_KEY,),
            include=_is_terrain_object,
            extra_args_for=_extra_args,
            label="terrain_output_bucket",
        )
        client = get_s3_hook(aws_conn_id).get_conn()
        summary = publication.publish(
            client,
            [plan],
            run_id=run_id,
            workers=DGM1_UPLOAD_WORKERS,
        )
        print(f"Published layer.json last to s3://{bucket}/{LAYER_KEY}")
        _write_json(
            job_dir / "dgm1_metadata" / "publication.json",
            {
                "bucket": bucket,
                "uploaded_terrain_files": len(plan.objects) - 1,
                "pruned_objects": summary[bucket]["pruned_objects"],
                "staged_and_verified_before_promotion": True,
                "layer_json_promoted_last": True,
            },
        )
        mf.update_step(str(job_dir), task_id, "success")
    except Exception as error:
        mf.update_step(str(job_dir), task_id, "failed", error=str(error))
        raise


def _is_terrain_object(key: str) -> bool:
    return key == LAYER_KEY or key.endswith(".terrain")


def _extra_args(key: str) -> dict:
    if key == LAYER_KEY:
        return {"ContentType": "application/json", "CacheControl": "no-cache"}
    return {
        "ContentType": TERRAIN_MEDIA_TYPE,
        "CacheControl": "public, max-age=31536000, immutable",
    }


def make_publish_terrain_task(aws_conn_id: str) -> PythonOperator:
    return PythonOperator(
        task_id="publish_terrain",
        python_callable=_publish_terrain_callable,
        op_kwargs={"task_id": "publish_terrain", "aws_conn_id": aws_conn_id},
    )


def _write_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    with temporary.open("w", encoding="utf-8") as file:
        json.dump(value, file, indent=2)
        file.write("\n")
    temporary.replace(path)
