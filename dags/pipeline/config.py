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
    "ghcr.io/csi-foxbyte/citygml-tools-docker:v2.4.1"
    "@sha256:8bd4b6e912d7adcb41f278f8adcb1906795c005024f4873a6512bf32a5562b4e"
)
ENRICH_IMAGE = (
    "ghcr.io/csi-foxbyte/regensburg_digitalerenergiezwilling_offlineenrichment:0.9.5"
    "@sha256:542d764dd4cf647864a98ad9ccdbd5b6c457db87f58d0f37987d871dedb81e0d"
)
JSON_TO_3D_TILES_IMAGE = (
    "ghcr.io/csi-foxbyte/cityjson-to-3d-tiles:0.0.26"
    "@sha256:a2fa31eba5dd54b10e4c28a5be226091b5ed57da933c053623f7929e6fbb16ec"
)
GDAL_IMAGE = (
    "ghcr.io/csi-foxbyte/regensburg-gdal:3.13.2-build-42047047116-1"
    "@sha256:3ede10ffb03a8a6dcdbfdf4facb24699f9efc453f1204f743372470b1b53b552"
)
CTB_IMAGE = (
    "ghcr.io/tum-gis/ctb-quantized-mesh"
    "@sha256:31053c0cee60fe2f6651812786b2861e1dda03b25a88f2fa6325a4a9e266ad98"
)
DOCKER_HOST = os.getenv("DOCKER_HOST", "unix://var/run/docker.sock")
BUNDLE_DIR = Path(__file__).resolve().parents[1]
DGM1_META4_PATH = os.getenv("DGM1_META4_PATH", str(BUNDLE_DIR / "dgm1.meta4"))
# Provider Metalinks that publish the reference size and SHA-256 of each DGM1
# tile (Bayerische Vermessungsverwaltung, per municipality / district). Every
# tile in DGM1_META4_PATH must be covered by one of them or carry its own hash
# (F-STAT-031); downloads that do not match are rejected.
DEFAULT_DGM1_REFERENCE_METALINKS = (
    "https://geodaten.bayern.de/odd/a/dgm/dgm1/meta/metalink/09362000.meta4",
    "https://geodaten.bayern.de/odd/a/dgm/dgm1/meta/metalink/09375.meta4",
    "https://geodaten.bayern.de/odd/a/dgm/dgm1/meta/metalink/09273.meta4",
)
_reference_metalinks = os.getenv("DGM1_REFERENCE_METALINKS")
DGM1_REFERENCE_METALINKS = (
    DEFAULT_DGM1_REFERENCE_METALINKS
    if _reference_metalinks is None
    else tuple(url.strip() for url in _reference_metalinks.split(",") if url.strip())
)
DGM1_DOWNLOAD_WORKERS = int(os.getenv("DGM1_DOWNLOAD_WORKERS", "4"))
DGM1_CTB_THREADS = int(os.getenv("DGM1_CTB_THREADS", "4"))
DGM1_UPLOAD_WORKERS = int(os.getenv("DGM1_UPLOAD_WORKERS", "8"))
# Concurrent S3 transfers when staging and promoting digital twin outputs.
PUBLISH_WORKERS = int(os.getenv("PUBLISH_WORKERS", "8"))
S3_CONN_ID = "det_rg_s3"
INPUT_ZIP_FILENAME = "input.zip"

# Safe-extraction limits for the untrusted input archive (F-STAT-003).
MAX_ZIP_ENTRIES = int(os.getenv("MAX_ZIP_ENTRIES", "50000"))
MAX_ZIP_TOTAL_BYTES = int(os.getenv("MAX_ZIP_TOTAL_BYTES", str(20 * 1024 ** 3)))
MAX_ZIP_RATIO = float(os.getenv("MAX_ZIP_RATIO", "1000"))

# S3 bucket parameters are supplied per run. They must look like a valid bucket
# name and be a member of the allowlist (F-STAT-017). Without an explicit
# DET_ALLOWED_BUCKETS the allowlist is the project's standard bucket set (the
# buckets created by the local stack and listed in .env.example), so run
# parameters can never target arbitrary buckets reachable with the credentials.
_BUCKET_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$")
DEFAULT_ALLOWED_BUCKETS = frozenset({
    "det-rg-upload",
    "det-rg-gml",
    "det-rg-json",
    "det-rg-main",
    "det-rg-terrain",
})


def _parse_allowed_buckets(value: str | None) -> frozenset[str]:
    configured = frozenset(
        name.strip() for name in (value or "").split(",") if name.strip()
    )
    return configured or DEFAULT_ALLOWED_BUCKETS


ALLOWED_BUCKETS = _parse_allowed_buckets(os.getenv("DET_ALLOWED_BUCKETS"))

def validate_bucket(bucket: str) -> str:
    """Validate a run-supplied S3 bucket name before it is read, cleared or written.

    Rejects malformed names and any bucket outside the allowlist
    (``DET_ALLOWED_BUCKETS``, or ``DEFAULT_ALLOWED_BUCKETS`` when unset) so an
    arbitrary bucket cannot be emptied or written to.
    """
    if not isinstance(bucket, str) or not _BUCKET_NAME_RE.match(bucket):
        raise ValueError(f"Invalid S3 bucket name: {bucket!r}")
    if bucket not in ALLOWED_BUCKETS:
        raise ValueError(
            f"Bucket {bucket!r} is not in the allowlist (DET_ALLOWED_BUCKETS)"
        )
    return bucket


def validate_distinct_buckets(buckets: dict[str, str]) -> None:
    """Reject runs that use one bucket for several roles.

    Publication prunes objects that are not part of the new output, so a
    shared bucket would delete the other role's data (e.g. the input ZIP).
    """
    seen: dict[str, str] = {}
    for role, bucket in buckets.items():
        if bucket in seen:
            raise ValueError(
                f"Bucket {bucket!r} is used for both {seen[bucket]} and {role}; "
                "use dedicated buckets"
            )
        seen[bucket] = role

WORK_DIR = _normalize_docker_host_path(os.getenv(
    "CITYJSON_WORK_DIR",
    "/opt/airflow/data/cityjson-to-3d-tiles",
))


def sanitize_job_id(run_id: str) -> str:
    return re.sub(r"[^\w\-]", "-", run_id).strip("-")


def get_job_dir(run_id: str) -> str:
    return os.path.join(WORK_DIR, "jobs", sanitize_job_id(run_id))
