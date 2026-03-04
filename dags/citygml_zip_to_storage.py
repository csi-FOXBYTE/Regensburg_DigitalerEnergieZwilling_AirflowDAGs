from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


DAG_ID = "citygml_zip_to_storage"
CITYJSON_IMAGE = "ghcr.io/csi-foxbyte/cityjson-to-3d-tiles:latest"
PREP_IMAGE = os.getenv("CITYGML_PREP_IMAGE", "python:3.12-slim")
UPLOAD_DIR_ON_HOST = os.getenv("CITYGML_UPLOAD_DIR", "/opt/airflow/data/uploads")
WORK_DIR_ON_HOST = os.getenv(
    "CITYJSON_WORK_DIR", "/opt/airflow/data/cityjson-to-3d-tiles"
)
STORAGE_DIR_ON_HOST = os.getenv("CITYJSON_STORAGE_DIR", "/opt/airflow/data/storage")
SRC_SRS = os.getenv("CITYJSON_SRC_SRS", "")
DEST_SRS = os.getenv("CITYJSON_DEST_SRS", "")


def _normalize_docker_host_path(path: str) -> str:
    if len(path) >= 3 and path[1] == ":" and path[2] in ("/", "\\"):
        drive = path[0].lower()
        rest = path[3:].replace("\\", "/")
        return f"/mnt/{drive}/{rest}"
    return path


cityjson_env = {
    "APPEARANCE": os.getenv("CITYJSON_APPEARANCE", "rgbTexture"),
    "THREAD_COUNT": os.getenv("CITYJSON_THREAD_COUNT", "4"),
    "HAS_ALPHA_ENABLED": os.getenv("CITYJSON_HAS_ALPHA_ENABLED", "true"),
    "SIMPLIFY_ADDRESSES": os.getenv("CITYJSON_SIMPLIFY_ADDRESSES", "false"),
    "SHOW_STACK_TRACE": os.getenv("CITYJSON_SHOW_STACK_TRACE", "false"),
}

if SRC_SRS:
    cityjson_env["SRC_SRS"] = SRC_SRS
if DEST_SRS:
    cityjson_env["DEST_SRS"] = DEST_SRS


extract_command = r"""
python - <<'PY'
import glob
import os
import shutil
import zipfile

uploads_dir = "/uploads"
work_dir = "/work"
incoming_dir = os.path.join(work_dir, "incoming")
zip_name = os.environ.get("ZIP_FILE", "").strip()

for pattern in ("*.gml", "*.xml", "*.citygml"):
    for file_path in glob.glob(os.path.join(work_dir, pattern)):
        os.remove(file_path)

for folder_name in ("incoming", "cityjson", "tiles"):
    shutil.rmtree(os.path.join(work_dir, folder_name), ignore_errors=True)
os.makedirs(incoming_dir, exist_ok=True)

if zip_name:
    zip_path = os.path.join(uploads_dir, zip_name)
    if not os.path.isfile(zip_path):
        raise FileNotFoundError(f"ZIP file not found: {zip_path}")
else:
    candidates = sorted(
        glob.glob(os.path.join(uploads_dir, "*.zip")), key=os.path.getmtime
    )
    if not candidates:
        raise FileNotFoundError(
            f"No ZIP files found in {uploads_dir}. Upload a ZIP and retrigger."
        )
    zip_path = candidates[-1]

print(f"Using ZIP file: {zip_path}")
with zipfile.ZipFile(zip_path) as archive:
    archive.extractall(incoming_dir)

citygml_files = []
for pattern in ("**/*.gml", "**/*.xml", "**/*.citygml"):
    citygml_files.extend(glob.glob(os.path.join(incoming_dir, pattern), recursive=True))

if not citygml_files:
    raise RuntimeError(
        "No CityGML-like files found in ZIP. Expected .gml/.xml/.citygml files."
    )

copied = 0
for source_path in citygml_files:
    base_name = os.path.basename(source_path)
    target_path = os.path.join(work_dir, base_name)
    if os.path.exists(target_path):
        stem, suffix = os.path.splitext(base_name)
        index = 1
        while os.path.exists(target_path):
            target_path = os.path.join(work_dir, f"{stem}_{index}{suffix}")
            index += 1
    shutil.copy2(source_path, target_path)
    copied += 1

shutil.rmtree(incoming_dir, ignore_errors=True)
print(f"Prepared {copied} CityGML file(s) in {work_dir}")
PY
"""


upload_command = r"""
python - <<'PY'
import os
import re
import shutil

tiles_dir = "/work/tiles"
storage_dir = "/storage"
run_id = os.environ.get("RUN_ID", "manual_run")
custom_subdir = os.environ.get("STORAGE_SUBDIR", "").strip()
target_name = custom_subdir if custom_subdir else run_id
safe_target_name = re.sub(r"[^A-Za-z0-9._-]+", "_", target_name)
target_dir = os.path.join(storage_dir, safe_target_name)

if not os.path.isdir(tiles_dir):
    raise RuntimeError(f"Tiles directory not found: {tiles_dir}")

entries = list(os.scandir(tiles_dir))
if not entries:
    raise RuntimeError(
        "No tile output files found in /work/tiles. Check previous task logs."
    )

os.makedirs(target_dir, exist_ok=True)
for entry in entries:
    src = entry.path
    dst = os.path.join(target_dir, entry.name)
    if entry.is_dir():
        shutil.copytree(src, dst, dirs_exist_ok=True)
    else:
        shutil.copy2(src, dst)

print(f"Copied tiles to: {target_dir}")
PY
"""


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["3d-tiles", "citygml", "zip", "storage"],
    params={
        "zip_file": Param(
            default="",
            type=["string", "null"],
            description="Optional ZIP filename from upload dir (e.g. buildings.zip).",
        ),
        "storage_subdir": Param(
            default="",
            type=["string", "null"],
            description="Optional target subfolder name for copied tile output.",
        ),
    },
) as dag:
    extract_citygml_zip = DockerOperator(
        task_id="extract_citygml_zip",
        image=PREP_IMAGE,
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=_normalize_docker_host_path(UPLOAD_DIR_ON_HOST),
                target="/uploads",
                type="bind",
            ),
            Mount(
                source=_normalize_docker_host_path(WORK_DIR_ON_HOST),
                target="/work",
                type="bind",
            ),
        ],
        environment={
            "ZIP_FILE": "{{ dag_run.conf.get('zip_file', '') }}",
        },
        command=extract_command,
        docker_url=os.getenv("DOCKER_HOST", "unix://var/run/docker.sock"),
    )

    generate_tiles = DockerOperator(
        task_id="generate_tiles",
        image=CITYJSON_IMAGE,
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=_normalize_docker_host_path(WORK_DIR_ON_HOST),
                target="/work",
                type="bind",
            )
        ],
        environment=cityjson_env,
        docker_url=os.getenv("DOCKER_HOST", "unix://var/run/docker.sock"),
    )

    upload_tiles_to_storage = DockerOperator(
        task_id="upload_tiles_to_storage",
        image=PREP_IMAGE,
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=_normalize_docker_host_path(WORK_DIR_ON_HOST),
                target="/work",
                type="bind",
            ),
            Mount(
                source=_normalize_docker_host_path(STORAGE_DIR_ON_HOST),
                target="/storage",
                type="bind",
            ),
        ],
        environment={
            "RUN_ID": "{{ run_id }}",
            "STORAGE_SUBDIR": "{{ dag_run.conf.get('storage_subdir', '') }}",
        },
        command=upload_command,
        docker_url=os.getenv("DOCKER_HOST", "unix://var/run/docker.sock"),
    )

    extract_citygml_zip >> generate_tiles >> upload_tiles_to_storage
