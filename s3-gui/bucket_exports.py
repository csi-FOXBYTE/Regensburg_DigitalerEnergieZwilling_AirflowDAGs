"""Background ZIP preparation for the single-process development S3 GUI."""

from contextlib import closing
import logging
import ntpath
from pathlib import Path
import tempfile
import threading
import time
from uuid import uuid4
import zipfile

from botocore.exceptions import (
    ConnectionClosedError,
    EndpointConnectionError,
    IncompleteReadError,
    ReadTimeoutError,
    ResponseStreamingError,
)


RETENTION_SECONDS = 24 * 60 * 60
DOWNLOAD_RANGE_SIZE = 8 * 1024 * 1024
logger = logging.getLogger(__name__)


def _read_range(client, bucket, key, start, end, size, etag):
    arguments = {"Bucket": bucket, "Key": key, "Range": f"bytes={start}-{end}"}
    if etag:
        arguments["IfMatch"] = etag
    for attempt in range(5):
        try:
            response = client.get_object(**arguments)
            with closing(response["Body"]) as body:
                expected = end - start + 1
                if (
                    response["ContentLength"] != expected
                    or response.get("ContentRange") != f"bytes {start}-{end}/{size}"
                ):
                    raise IOError("S3 returned an unexpected object range")
                chunks = []
                remaining = expected
                while remaining:
                    chunk = body.read(min(256 * 1024, remaining))
                    if not chunk:
                        raise IOError("S3 returned an incomplete object range")
                    chunks.append(chunk)
                    remaining -= len(chunk)
                return b"".join(chunks)
        except (OSError, ConnectionClosedError, EndpointConnectionError, IncompleteReadError, ReadTimeoutError, ResponseStreamingError):
            if attempt == 4:
                raise
            time.sleep(min(2 ** attempt, 8))


def _validate_archive_key(key):
    # Reject names that ZIP readers might truncate, normalize, or extract outside
    # the destination. Renaming them silently could collide with another object.
    parts = key.removesuffix("/").split("/")
    if (
        not key
        or "\x00" in key
        or "\\" in key
        or ntpath.splitdrive(key)[0]
        or any(part in {"", ".", ".."} for part in parts)
    ):
        raise ValueError(f"Object key cannot be represented safely in a ZIP: {key!r}")


class ExportBusyError(Exception):
    pass


class BucketExports:
    def __init__(self, client_factory):
        self.client_factory = client_factory
        self._lock = threading.Lock()
        self._jobs = {}
        self._directory = None

    def start(self, bucket):
        with self._lock:
            for job in self._jobs.values():
                if job["status"] == "preparing":
                    if job["bucket"] == bucket:
                        return job.copy()
                    raise ExportBusyError("Another bucket export is still being prepared.")
            if self._directory is None:
                self._directory = tempfile.TemporaryDirectory(prefix="s3-gui-exports-")
            job = {
                "id": uuid4().hex,
                "bucket": bucket,
                "status": "preparing",
                "objects": 0,
                "bytes": 0,
                "error": None,
                "expires_at": None,
            }
            self._jobs[job["id"]] = job
            result = job.copy()
        worker = threading.Thread(target=self._prepare, args=(job["id"],), daemon=True)
        try:
            worker.start()
        except Exception as exc:
            self._update(job["id"], status="failed", error=str(exc), expires_at=time.time() + RETENTION_SECONDS)
            raise
        return result

    def get(self, job_id, *, retain=False):
        with self._lock:
            job = self._jobs.get(job_id)
            if job and retain and job["status"] == "ready":
                job["expires_at"] = time.time() + RETENTION_SECONDS
            return job.copy() if job else None

    def latest(self, bucket):
        with self._lock:
            for job in reversed(self._jobs.values()):
                if job["bucket"] == bucket:
                    return job.copy()
        return None

    def path(self, job_id):
        # Only call for a job already found in the registry, never a raw URL ID.
        return Path(self._directory.name) / f"{job_id}.zip"

    def cleanup_expired(self):
        with self._lock:
            for job_id, job in list(self._jobs.items()):
                if job["expires_at"] is not None and job["expires_at"] <= time.time():
                    try:
                        self.path(job_id).unlink(missing_ok=True)
                    except OSError:
                        logger.exception("Could not remove expired bucket export %s", job_id)
                        continue
                    del self._jobs[job_id]

    def _update(self, job_id, **values):
        with self._lock:
            self._jobs[job_id].update(values)

    def _progress(self, job_id, byte_count):
        with self._lock:
            self._jobs[job_id]["bytes"] += byte_count

    def _prepare(self, job_id):
        job = self.get(job_id)
        path = self.path(job_id)
        # No compression: terrain and other large assets export quickly, with
        # bounded transfer buffers. ZIP64 supports large files and object counts.
        try:
            with closing(self.client_factory()) as client, zipfile.ZipFile(path, "w") as archive:
                pages = client.get_paginator("list_objects_v2").paginate(Bucket=job["bucket"])
                completed = 0
                for page in pages:
                    for item in page.get("Contents", []):
                        key = item["Key"]
                        _validate_archive_key(key)
                        metadata = client.head_object(Bucket=job["bucket"], Key=key)
                        size = metadata["ContentLength"]
                        with archive.open(key, "w", force_zip64=True) as destination:
                            for start in range(0, size, DOWNLOAD_RANGE_SIZE):
                                data = _read_range(
                                    client, job["bucket"], key, start,
                                    min(start + DOWNLOAD_RANGE_SIZE, size) - 1,
                                    size, metadata.get("ETag"),
                                )
                                destination.write(data)
                                self._progress(job_id, len(data))
                        completed += 1
                        self._update(job_id, objects=completed)
            self._update(job_id, status="ready", expires_at=time.time() + RETENTION_SECONDS)
        except Exception as exc:
            logger.exception("Bucket export %s failed", job_id)
            try:
                path.unlink(missing_ok=True)
            except OSError:
                logger.exception("Could not remove incomplete bucket export %s", job_id)
            self._update(job_id, status="failed", error=str(exc), expires_at=time.time() + RETENTION_SECONDS)
