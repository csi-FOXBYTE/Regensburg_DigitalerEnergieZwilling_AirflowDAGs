import os
import re
from pathlib import Path


def _normalize_docker_host_path(path: str) -> str:
    """Normalize Windows paths for Linux Docker daemon inside Airflow containers."""
    if len(path) >= 3 and path[1] == ":" and path[2] in ("/", "\\"):
        drive = path[0].lower()
        rest = path[3:].replace("\\", "/")
        return f"/mnt/{drive}/{rest}"
    return path


GML_TOOLS_IMAGE = "ghcr.io/csi-foxbyte/citygml-tools-docker:latest"
ENRICH_IMAGE = (
    "ghcr.io/csi-foxbyte/regensburg_digitalerenergiezwilling_offlineenrichment:0.7.0"
    "@sha256:d42bd14722678060c18235e9979cf47768f163fb90771361981ca87f853eef15"
)
JSON_TO_3D_TILES_IMAGE = (
    "ghcr.io/csi-foxbyte/cityjson-to-3d-tiles:0.0.24"
    "@sha256:39ab45799580d32d73ff58bc421df585b77e3b8fd5ec8e9dd8a9cf940b68e038"
)
GDAL_IMAGE = (
    "ghcr.io/osgeo/gdal:alpine-small-3.13.2"
    "@sha256:9f453a4c7af9862cce78831d7cb587f8e7b98ff4dbbc177d6b6e213e20260e0e"
)
CTB_IMAGE = (
    "ghcr.io/tum-gis/ctb-quantized-mesh:latest"
    "@sha256:31053c0cee60fe2f6651812786b2861e1dda03b25a88f2fa6325a4a9e266ad98"
)
DOCKER_HOST = os.getenv("DOCKER_HOST", "unix://var/run/docker.sock")
PROJECT_DIR = Path(__file__).resolve().parents[2]
DGM1_META4_PATH = os.getenv("DGM1_META4_PATH", str(PROJECT_DIR / "dgm1.meta4"))
DGM1_DOWNLOAD_WORKERS = int(os.getenv("DGM1_DOWNLOAD_WORKERS", "4"))
DGM1_CTB_THREADS = int(os.getenv("DGM1_CTB_THREADS", "4"))
DGM1_UPLOAD_WORKERS = int(os.getenv("DGM1_UPLOAD_WORKERS", "8"))
S3_CONN_ID = "det_rg_s3"

WORK_DIR = _normalize_docker_host_path(os.getenv(
    "CITYJSON_WORK_DIR",
    "/opt/airflow/data/cityjson-to-3d-tiles",
))


def sanitize_job_id(run_id: str) -> str:
    return re.sub(r"[^\w\-]", "-", run_id).strip("-")


def get_job_dir(run_id: str) -> str:
    return os.path.join(WORK_DIR, "jobs", sanitize_job_id(run_id))
