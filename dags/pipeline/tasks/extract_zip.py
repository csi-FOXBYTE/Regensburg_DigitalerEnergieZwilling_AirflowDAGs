import os
import stat
import zipfile
from airflow.providers.standard.operators.python import PythonOperator
from pipeline.config import (
    INPUT_ZIP_FILENAME,
    MAX_ZIP_ENTRIES,
    MAX_ZIP_RATIO,
    MAX_ZIP_TOTAL_BYTES,
    get_job_dir,
)
from pipeline import manifest as mf


def _safe_extract(zip_file: zipfile.ZipFile, dest_dir: str) -> None:
    """Extract an untrusted ZIP archive defensively.

    Guards against path traversal (entries that escape ``dest_dir``), symlink
    entries, and decompression bombs (entry count, total uncompressed size, and
    per-entry compression ratio). Limits are configurable via the ``MAX_ZIP_*``
    environment variables.
    """
    dest_root = os.path.realpath(dest_dir)
    total_uncompressed = 0
    for index, info in enumerate(zip_file.infolist()):
        if index + 1 > MAX_ZIP_ENTRIES:
            raise ValueError(f"Archive has too many entries (> {MAX_ZIP_ENTRIES})")

        target = os.path.realpath(os.path.join(dest_root, info.filename))
        if target != dest_root and not target.startswith(dest_root + os.sep):
            raise ValueError(f"Unsafe path in archive: {info.filename!r}")

        if stat.S_ISLNK(info.external_attr >> 16):
            raise ValueError(f"Symlink entries are not allowed: {info.filename!r}")

        total_uncompressed += info.file_size
        if total_uncompressed > MAX_ZIP_TOTAL_BYTES:
            raise ValueError(
                f"Archive uncompressed size exceeds {MAX_ZIP_TOTAL_BYTES} bytes"
            )

        if info.compress_size > 0 and info.file_size / info.compress_size > MAX_ZIP_RATIO:
            raise ValueError(
                f"Compression ratio too high for entry {info.filename!r} "
                f"(> {MAX_ZIP_RATIO})"
            )

    zip_file.extractall(dest_dir)


def _extract_zip_callable(params, run_id, task_id):
    job_dir = get_job_dir(run_id)
    if not params.get("key"):
        raise ValueError("Missing param: key")
    mf.update_step(job_dir, task_id, "running")
    try:
        zip_path = os.path.join(job_dir, "zip", INPUT_ZIP_FILENAME)
        if not os.path.isfile(zip_path):
            raise FileNotFoundError(f"Zip file not found: {zip_path}")
        with zipfile.ZipFile(zip_path, "r") as z:
            _safe_extract(z, os.path.join(job_dir, "gml_in"))
        mf.update_step(job_dir, task_id, "success")
    except Exception as e:
        mf.update_step(job_dir, task_id, "failed", error=str(e))
        raise


def make_extract_zip_task() -> PythonOperator:
    return PythonOperator(
        task_id="extract_zip",
        python_callable=_extract_zip_callable,
        op_kwargs={"task_id": "extract_zip"},
    )
