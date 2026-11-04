"""Staged, verified publication of pipeline outputs to S3 (F-STAT-005).

Consumers (3D viewer, Cesium terrain provider, CityGML download) read the
output buckets at their root, e.g. ``<tiles_url>/tileset.json`` and
``<terrain_url>/layer.json``. A versioned prefix would therefore require
consumer changes, so publication keeps the bucket-root layout and replaces its
contents in phases instead of "clear bucket, then upload":

1. *stage*: upload every new object below ``.det-publication/staging/<job>/``
   in the target bucket. Live objects are not touched.
2. *verify*: list the staging prefix and require exactly the planned key set
   with the expected sizes. An incomplete upload is never promoted.
3. *promote*: server-side copy staged objects to their live keys. Entry points
   (``tileset.json``, ``layer.json``) are copied last so clients do not discover
   a new root document before the content it references exists.
4. *prune*: only after every bucket of the run is promoted, delete live objects
   that are not part of the new set, plus the staging data.
5. *status*: write ``.det-publication/status.json`` describing the outcome
   (state, run id, live content state, last successful publication).

On failure the run's staging data is removed and a ``failed`` status is
written; the previous publication stays live and is never emptied.
"""

import concurrent.futures
import datetime
import itertools
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable


CONTROL_PREFIX = ".det-publication/"
STAGING_PREFIX = f"{CONTROL_PREFIX}staging/"
STATUS_KEY = f"{CONTROL_PREFIX}status.json"
# CopyObject handles at most 5 GiB; larger objects use the managed copy.
_MAX_COPY_OBJECT_BYTES = 5 * 1024 ** 3
_DELETE_BATCH = 1000


class PublicationError(RuntimeError):
    """Raised when outputs are incomplete or could not be published safely."""


@dataclass(frozen=True)
class PlannedObject:
    key: str
    path: str
    size: int
    extra_args: dict | None = None


@dataclass
class PublicationPlan:
    """The complete object set that replaces the contents of one bucket."""

    bucket: str
    job_id: str
    objects: dict[str, PlannedObject]
    entry_keys: tuple[str, ...] = ()
    label: str = ""
    state: str = field(default="unchanged", init=False)

    @property
    def staging_prefix(self) -> str:
        return f"{STAGING_PREFIX}{self.job_id}/"

    def staging_key(self, key: str) -> str:
        return f"{self.staging_prefix}{key}"

    @property
    def total_bytes(self) -> int:
        return sum(item.size for item in self.objects.values())


def build_plan(
    bucket: str,
    job_id: str,
    directories: Iterable[str | os.PathLike],
    *,
    required_keys: Iterable[str] = (),
    entry_keys: Iterable[str] = (),
    include: Callable[[str], bool] | None = None,
    extra_args_for: Callable[[str], dict | None] | None = None,
    label: str = "",
) -> PublicationPlan:
    """Collect the local output files that make up a complete publication.

    Object keys are paths relative to each source directory, so the bucket
    layout matches the previous direct uploads. ``include`` filters keys,
    ``extra_args_for`` supplies upload arguments (content type, caching).
    """
    if not job_id or "/" in job_id:
        raise ValueError(f"Invalid job id for staging: {job_id!r}")
    objects: dict[str, PlannedObject] = {}
    for directory in directories:
        directory = Path(directory)
        if not directory.is_dir():
            raise PublicationError(f"Output directory does not exist: {directory}")
        for path in sorted(p for p in directory.rglob("*") if p.is_file()):
            key = path.relative_to(directory).as_posix()
            if include and not include(key):
                continue
            if key.startswith(CONTROL_PREFIX):
                raise PublicationError(f"Output uses reserved key prefix: {key}")
            if key in objects:
                raise PublicationError(f"Duplicate output key {key!r} for s3://{bucket}/")
            extra_args = extra_args_for(key) if extra_args_for else None
            objects[key] = PlannedObject(key, str(path), path.stat().st_size, extra_args)
    if not objects:
        raise PublicationError(f"No output files to publish to s3://{bucket}/")
    missing = [key for key in (*required_keys, *entry_keys) if key not in objects]
    if missing:
        raise PublicationError(
            f"Output for s3://{bucket}/ is incomplete; missing {sorted(set(missing))}"
        )
    return PublicationPlan(
        bucket=bucket,
        job_id=job_id,
        objects=objects,
        entry_keys=tuple(entry_keys),
        label=label or bucket,
    )


def publish(client, plans: list[PublicationPlan], *, run_id: str, workers: int = 8) -> dict:
    """Publish all plans together; returns a summary per bucket.

    Every plan is staged and verified before any live object changes, and no
    bucket is pruned before every bucket has been promoted. Promotion follows
    the order of ``plans``.
    """
    buckets = [plan.bucket for plan in plans]
    if len(set(buckets)) != len(buckets):
        raise PublicationError(f"Each output needs its own bucket: {buckets}")
    previous = {plan.bucket: _read_status(client, plan.bucket) for plan in plans}
    phase = "staging"
    try:
        for plan in plans:
            stage(client, plan, workers=workers)
        phase = "verifying"
        for plan in plans:
            verify_staging(client, plan)
        phase = "promoting"
        for plan in plans:
            promote(client, plan, workers=workers)
        phase = "pruning"
        pruned = {plan.bucket: prune(client, plan) for plan in plans}
    except Exception as error:
        for plan in plans:
            _best_effort(f"discard staging in s3://{plan.bucket}/", discard_staging, client, plan)
            _best_effort(
                f"write status to s3://{plan.bucket}/",
                write_status,
                client,
                plan.bucket,
                _failed_status(plan, run_id, phase, error, previous[plan.bucket]),
            )
        raise

    summary = {}
    for plan in plans:
        status = _published_status(plan, run_id, pruned[plan.bucket])
        write_status(client, plan.bucket, status)
        summary[plan.bucket] = status
        print(
            f"Published {len(plan.objects)} object(s) to s3://{plan.bucket}/ "
            f"and pruned {pruned[plan.bucket]} stale object(s)"
        )
    return summary


def stage(client, plan: PublicationPlan, *, workers: int = 8) -> None:
    def upload(item: PlannedObject) -> None:
        kwargs = {"ExtraArgs": item.extra_args} if item.extra_args else {}
        client.upload_file(item.path, plan.bucket, plan.staging_key(item.key), **kwargs)

    _run_bounded(upload, list(plan.objects.values()), workers)
    print(f"Staged {len(plan.objects)} object(s) in s3://{plan.bucket}/{plan.staging_prefix}")


def verify_staging(client, plan: PublicationPlan) -> int:
    """Require exactly the planned objects (keys and sizes) in staging."""
    staged = {
        item["Key"][len(plan.staging_prefix):]: item["Size"]
        for item in _list_objects(client, plan.bucket, plan.staging_prefix)
    }
    missing = sorted(set(plan.objects) - set(staged))
    unexpected = sorted(set(staged) - set(plan.objects))
    wrong_size = sorted(
        key for key, item in plan.objects.items()
        if key in staged and staged[key] != item.size
    )
    if missing or unexpected or wrong_size:
        raise PublicationError(
            f"Staged output in s3://{plan.bucket}/ is incomplete: "
            f"{len(missing)} missing (e.g. {missing[:3]}), "
            f"{len(unexpected)} unexpected (e.g. {unexpected[:3]}), "
            f"{len(wrong_size)} with wrong size (e.g. {wrong_size[:3]})"
        )
    print(f"Verified {len(staged)} staged object(s) in s3://{plan.bucket}/")
    return len(staged)


def promote(client, plan: PublicationPlan, *, workers: int = 8) -> None:
    def copy(item: PlannedObject) -> None:
        source = {"Bucket": plan.bucket, "Key": plan.staging_key(item.key)}
        if item.size <= _MAX_COPY_OBJECT_BYTES:
            client.copy_object(
                Bucket=plan.bucket,
                Key=item.key,
                CopySource=source,
                MetadataDirective="COPY",
            )
        else:
            client.copy(source, plan.bucket, item.key)

    entry = [plan.objects[key] for key in plan.entry_keys]
    content = [item for key, item in plan.objects.items() if key not in plan.entry_keys]
    plan.state = "partially_replaced"
    _run_bounded(copy, content, workers)
    for item in entry:
        copy(item)
    plan.state = "replaced"
    print(f"Promoted {len(plan.objects)} object(s) in s3://{plan.bucket}/")


def prune(client, plan: PublicationPlan) -> int:
    """Delete live objects outside the new set, including any staging data."""
    stale = [
        item["Key"]
        for item in _list_objects(client, plan.bucket, "")
        if item["Key"] not in plan.objects and item["Key"] != STATUS_KEY
    ]
    _delete_keys(client, plan.bucket, stale)
    plan.state = "replaced_and_pruned"
    return sum(1 for key in stale if not key.startswith(CONTROL_PREFIX))


def discard_staging(client, plan: PublicationPlan) -> int:
    keys = [item["Key"] for item in _list_objects(client, plan.bucket, plan.staging_prefix)]
    _delete_keys(client, plan.bucket, keys)
    if keys:
        print(f"Discarded {len(keys)} staged object(s) from s3://{plan.bucket}/{plan.staging_prefix}")
    return len(keys)


def write_status(client, bucket: str, status: dict) -> None:
    client.put_object(
        Bucket=bucket,
        Key=STATUS_KEY,
        Body=(json.dumps(status, indent=2) + "\n").encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-cache",
    )


def _published_status(plan: PublicationPlan, run_id: str, pruned: int) -> dict:
    now = _now()
    publication = {
        "run_id": run_id,
        "job_id": plan.job_id,
        "published_at": now,
        "objects": len(plan.objects),
        "bytes": plan.total_bytes,
        "entry_points": list(plan.entry_keys),
    }
    return {
        "state": "published",
        "updated_at": now,
        "output": plan.label,
        "run_id": run_id,
        "live_content": "complete",
        "pruned_objects": pruned,
        "last_published": publication,
    }


def _failed_status(
    plan: PublicationPlan,
    run_id: str,
    phase: str,
    error: Exception,
    previous: dict | None,
) -> dict:
    return {
        "state": "failed",
        "updated_at": _now(),
        "output": plan.label,
        "run_id": run_id,
        "failed_phase": phase,
        # Output buckets may be publicly readable: record only the error type
        # here; the message is kept in the run's local manifest.json.
        "error_type": type(error).__name__,
        # unchanged: previous publication fully live; partially_replaced /
        # replaced: some or all new objects are live next to stale ones.
        "live_content": plan.state,
        "last_published": (previous or {}).get("last_published"),
    }


def _read_status(client, bucket: str) -> dict | None:
    try:
        response = client.get_object(Bucket=bucket, Key=STATUS_KEY)
        return json.loads(response["Body"].read())
    except Exception:
        return None


def _list_objects(client, bucket: str, prefix: str):
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", [])


def _delete_keys(client, bucket: str, keys: list[str]) -> None:
    for start in range(0, len(keys), _DELETE_BATCH):
        batch = keys[start:start + _DELETE_BATCH]
        response = client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
        )
        errors = response.get("Errors", [])
        if errors:
            raise PublicationError(f"Failed to delete objects in s3://{bucket}/: {errors[:3]}")


def _run_bounded(function, items: list, workers: int) -> None:
    """Run ``function`` over ``items`` with bounded concurrency; fail fast."""
    if not items:
        return
    workers = max(1, min(workers, len(items)))
    iterator = iter(items)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        pending = {
            executor.submit(function, item)
            for item in itertools.islice(iterator, workers * 2)
        }
        while pending:
            done, pending = concurrent.futures.wait(
                pending, return_when=concurrent.futures.FIRST_EXCEPTION
            )
            for future in done:
                error = future.exception()
                if error is not None:
                    for other in pending:
                        other.cancel()
                    raise error
            for item in itertools.islice(iterator, len(done)):
                pending.add(executor.submit(function, item))


def _best_effort(description: str, function, *args) -> None:
    try:
        function(*args)
    except Exception as error:  # keep the original publication error
        print(f"Could not {description}: {error}")


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
