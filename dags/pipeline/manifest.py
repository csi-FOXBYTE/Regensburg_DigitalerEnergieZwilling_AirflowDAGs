import fcntl
import json
import os
import datetime

from pipeline.config import WORK_DIR


ARTIFACT_DIRS = ["json", "enriched_json", "gml_in", "gml_out", "3d_tiles", "address_db", "gpkg", "zip"]


def _discover_artifacts(job_dir: str) -> dict[str, str]:
    rel = os.path.relpath(job_dir, WORK_DIR)
    artifacts = {}
    for directory in ARTIFACT_DIRS:
        path = os.path.join(job_dir, directory)
        if os.path.isdir(path) and any(files for _root, _dirs, files in os.walk(path)):
            artifacts[directory] = f"{rel}/{directory}/"
    return artifacts


def create_manifest(
    job_dir: str,
    job_id: str,
    run_id: str,
    dag_id: str,
    params: dict,
    step_names: list[str],
) -> None:
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
        "artifacts": _discover_artifacts(job_dir),
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
        m["artifacts"] = _discover_artifacts(job_dir)
        f.seek(0)
        f.truncate()
        json.dump(m, f, indent=2)
        fcntl.flock(f, fcntl.LOCK_UN)


def finalize_manifest(job_dir: str, step_name: str = "finalize_manifest") -> None:
    path = os.path.join(job_dir, "manifest.json")
    with open(path, "r+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        m = json.load(f)
        now = _now()
        final_step = m["steps"].setdefault(step_name, {})
        final_step["status"] = "success"
        final_step.setdefault("started_at", now)
        final_step["finished_at"] = now
        for step in m["steps"].values():
            if step.get("status") == "pending":
                step["status"] = "skipped"
        failed = any(s.get("status") == "failed" for s in m["steps"].values())
        m["status"] = "failed" if failed else "success"
        m["finished_at"] = now
        m["artifacts"] = _discover_artifacts(job_dir)
        f.seek(0)
        f.truncate()
        json.dump(m, f, indent=2)
        fcntl.flock(f, fcntl.LOCK_UN)


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
