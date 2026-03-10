import os

def _normalize_docker_host_path(path: str) -> str:
    """Normalize Windows paths for Linux Docker daemon inside Airflow containers."""
    if len(path) >= 3 and path[1] == ":" and path[2] in ("/", "\\"):
        drive = path[0].lower()
        rest = path[3:].replace("\\", "/")
        return f"/mnt/{drive}/{rest}"
    return path

GML_TOOLS_IMAGE = "ghcr.io/csi-foxbyte/citygml-tools-docker:latest"
DOCKER_HOST = os.getenv("DOCKER_HOST", "unix://var/run/docker.sock")
WORK_DIR = _normalize_docker_host_path(os.getenv(
    "CITYJSON_WORK_DIR",
    "/opt/airflow/data/cityjson-to-3d-tiles",
))
