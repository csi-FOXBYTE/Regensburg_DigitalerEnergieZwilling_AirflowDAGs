import concurrent.futures
import json
from pathlib import Path

from airflow.providers.standard.operators.python import PythonOperator

from pipeline import manifest as mf
from pipeline.config import DGM1_UPLOAD_WORKERS, get_job_dir
from pipeline.s3_connection import get_s3_hook
from pipeline.terrain_validation import TERRAIN_MEDIA_TYPE


def _publish_terrain_callable(params, run_id, task_id, aws_conn_id):
    job_dir = Path(get_job_dir(run_id))
    mf.update_step(str(job_dir), task_id, "running")
    try:
        bucket = params.get("terrain_output_bucket")
        if not bucket:
            raise ValueError("Missing param: terrain_output_bucket")
        terrain_dir = job_dir / "terrain"
        layer_path = terrain_dir / "layer.json"
        tile_paths = sorted(terrain_dir.rglob("*.terrain"))
        if not layer_path.is_file() or not tile_paths:
            raise FileNotFoundError("Validated terrain output is incomplete")

        hook = get_s3_hook(aws_conn_id)
        client = hook.get_conn()
        deleted_count = _clear_bucket(client, bucket)
        print(f"Cleared {deleted_count} object(s) from s3://{bucket}/")

        worker_count = max(1, min(DGM1_UPLOAD_WORKERS, len(tile_paths)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
            pending = set()
            paths = iter(tile_paths)
            for _ in range(min(worker_count * 2, len(tile_paths))):
                pending.add(
                    executor.submit(
                        _upload_tile,
                        client,
                        bucket,
                        terrain_dir,
                        next(paths),
                    )
                )
            uploaded_count = 0
            while pending:
                done, pending = concurrent.futures.wait(
                    pending,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    future.result()
                    uploaded_count += 1
                    try:
                        path = next(paths)
                    except StopIteration:
                        continue
                    pending.add(
                        executor.submit(
                            _upload_tile,
                            client,
                            bucket,
                            terrain_dir,
                            path,
                        )
                    )
                if uploaded_count % 1000 == 0 or uploaded_count == len(tile_paths):
                    print(f"Uploaded {uploaded_count}/{len(tile_paths)} terrain tiles")

        client.upload_file(
            str(layer_path),
            bucket,
            "layer.json",
            ExtraArgs={
                "ContentType": "application/json",
                "CacheControl": "no-cache",
            },
        )
        print(f"Published layer.json last to s3://{bucket}/layer.json")
        _write_json(
            job_dir / "dgm1_metadata" / "publication.json",
            {
                "bucket": bucket,
                "cleared_objects": deleted_count,
                "uploaded_terrain_files": len(tile_paths),
                "layer_json_uploaded_last": True,
            },
        )
        mf.update_step(str(job_dir), task_id, "success")
    except Exception as error:
        mf.update_step(str(job_dir), task_id, "failed", error=str(error))
        raise


def _clear_bucket(client, bucket: str) -> int:
    deleted_count = 0
    while True:
        response = client.list_objects_v2(Bucket=bucket, MaxKeys=1000)
        objects = response.get("Contents", [])
        if not objects:
            return deleted_count
        delete_response = client.delete_objects(
            Bucket=bucket,
            Delete={
                "Objects": [{"Key": item["Key"]} for item in objects],
                "Quiet": True,
            },
        )
        errors = delete_response.get("Errors", [])
        if errors:
            raise RuntimeError(f"Failed to clear s3://{bucket}/: {errors}")
        deleted_count += len(objects)


def _upload_tile(client, bucket: str, terrain_dir: Path, tile_path: Path) -> None:
    key = tile_path.relative_to(terrain_dir).as_posix()
    client.upload_file(
        str(tile_path),
        bucket,
        key,
        ExtraArgs={
            "ContentType": TERRAIN_MEDIA_TYPE,
            "CacheControl": "public, max-age=31536000, immutable",
        },
    )


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
