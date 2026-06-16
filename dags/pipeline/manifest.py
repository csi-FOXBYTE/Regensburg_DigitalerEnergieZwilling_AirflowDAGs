import fcntl
import json
import os
import datetime

from pipeline.config import WORK_DIR


def create_manifest(
    job_dir: str,
    job_id: str,
    run_id: str,
    dag_id: str,
    params: dict,
    step_names: list[str],
) -> None:
    rel = os.path.relpath(job_dir, WORK_DIR)
    manifest = {
        "job_id": job_id,
        "run_id": run_id,
        "dag_id": dag_id,
        "status": "running",
        "update_scope": {
            "source": f"s3://{params.get('bucket')}/{params.get('key')}" if params.get("bucket") else None,
            "tiles_output_bucket": params.get("tiles_output_bucket"),
            "gml_output_bucket": params.get("gml_output_bucket"),
        },
        "started_at": _now(),
        "finished_at": None,
        "inputs": params,
        "steps": {name: {"status": "pending"} for name in step_names},
        "artifacts": {
            d: f"{rel}/{d}/"
            for d in ["json", "enriched_json", "gml_in", "gml_out", "3d_tiles", "address_db", "gpkg", "zip"]
        },
        "errors": [],
    }
    with open(os.path.join(job_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)


def update_step(job_dir: str, step_name: str, status: str, error: str | None = None) -> None:
    path = os.path.join(job_dir, "manifest.json")
    with open(path, "r+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        m = json.load(f)
        now = _now()
        step = m["steps"].setdefault(step_name, {})
        if status == "running":
            step["started_at"] = now
        if status in ("success", "failed"):
            step["finished_at"] = now
        step["status"] = status
        if error:
            m["errors"].append({"step": step_name, "error": error})
        f.seek(0)
        f.truncate()
        json.dump(m, f, indent=2)
        fcntl.flock(f, fcntl.LOCK_UN)


def finalize_manifest(job_dir: str) -> None:
    path = os.path.join(job_dir, "manifest.json")
    with open(path, "r+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        m = json.load(f)
        for step in m["steps"].values():
            if step.get("status") == "pending":
                step["status"] = "skipped"
        failed = any(s.get("status") == "failed" for s in m["steps"].values())
        m["status"] = "failed" if failed else "success"
        m["finished_at"] = _now()
        f.seek(0)
        f.truncate()
        json.dump(m, f, indent=2)
        fcntl.flock(f, fcntl.LOCK_UN)


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
