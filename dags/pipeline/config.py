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


GML_TOOLS_IMAGE = (
    "ghcr.io/csi-foxbyte/citygml-tools-docker:v2.4.0"
    "@sha256:2ab31f2226775bdd74f0640be8b69f87effa8d1e840b5f80e10ac08c7bfd82cb"
)
ENRICH_IMAGE = (
    "ghcr.io/csi-foxbyte/regensburg_digitalerenergiezwilling_offlineenrichment:0.9.4"
    "@sha256:dcbaede979d8de476e4e4f3d1fee1b2f8707cf850c4b1b790b9b33caef2764c9"
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
    "ghcr.io/tum-gis/ctb-quantized-mesh"
    "@sha256:31053c0cee60fe2f6651812786b2861e1dda03b25a88f2fa6325a4a9e266ad98"
)
DOCKER_HOST = os.getenv("DOCKER_HOST", "unix://var/run/docker.sock")
BUNDLE_DIR = Path(__file__).resolve().parents[1]
DGM1_META4_PATH = os.getenv("DGM1_META4_PATH", str(BUNDLE_DIR / "dgm1.meta4"))
DGM1_DOWNLOAD_WORKERS = int(os.getenv("DGM1_DOWNLOAD_WORKERS", "4"))
DGM1_CTB_THREADS = int(os.getenv("DGM1_CTB_THREADS", "4"))
DGM1_UPLOAD_WORKERS = int(os.getenv("DGM1_UPLOAD_WORKERS", "8"))
S3_CONN_ID = "det_rg_s3"
INPUT_ZIP_FILENAME = "input.zip"

# Safe-extraction limits for the untrusted input archive (F-STAT-003).
MAX_ZIP_ENTRIES = int(os.getenv("MAX_ZIP_ENTRIES", "50000"))
MAX_ZIP_TOTAL_BYTES = int(os.getenv("MAX_ZIP_TOTAL_BYTES", str(20 * 1024 ** 3)))
MAX_ZIP_RATIO = float(os.getenv("MAX_ZIP_RATIO", "1000"))

# S3 bucket parameters are supplied per run. They must look like a valid bucket
# name and, when an allowlist is configured, be a member of it (F-STAT-017).
_BUCKET_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$")
ALLOWED_BUCKETS = frozenset(
    name.strip()
    for name in os.getenv("DET_ALLOWED_BUCKETS", "").split(",")
    if name.strip()
)


def validate_bucket(bucket: str) -> str:
    """Validate a run-supplied S3 bucket name before it is cleared or written.

    Rejects malformed names and, when ``DET_ALLOWED_BUCKETS`` is set, any bucket
    outside the allowlist so an arbitrary bucket cannot be emptied or written to.
    """
    if not _BUCKET_NAME_RE.match(bucket):
        raise ValueError(f"Invalid S3 bucket name: {bucket!r}")
    if ALLOWED_BUCKETS and bucket not in ALLOWED_BUCKETS:
        raise ValueError(
            f"Bucket {bucket!r} is not in the allowlist (DET_ALLOWED_BUCKETS)"
        )
    return bucket

WORK_DIR = _normalize_docker_host_path(os.getenv(
    "CITYJSON_WORK_DIR",
    "/opt/airflow/data/cityjson-to-3d-tiles",
))


def sanitize_job_id(run_id: str) -> str:
    return re.sub(r"[^\w\-]", "-", run_id).strip("-")


def get_job_dir(run_id: str) -> str:
    return os.path.join(WORK_DIR, "jobs", sanitize_job_id(run_id))
