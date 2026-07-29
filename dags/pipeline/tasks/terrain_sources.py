import concurrent.futures
import json
import math
import os
import re
import shutil
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlparse

from airflow.models import BaseOperator
from airflow.providers.standard.operators.python import PythonOperator

from pipeline import manifest as mf
from pipeline.config import (
    DGM1_DOWNLOAD_WORKERS,
    DGM1_META4_PATH,
    DOCKER_HOST,
    GDAL_IMAGE,
    get_job_dir,
)
from pipeline.operators.job_docker_operator import JobDockerOperator


EXPECTED_SOURCE_COUNT = 368
DOWNLOAD_CHUNK_SIZE = 1024 * 1024
DOWNLOAD_ATTEMPTS = 4
SOURCE_NAME_PATTERN = re.compile(r"^\d+_\d+\.tif$")


GDAL_INSPECTION_COMMAND = """
set -eu
mkdir -p /work/dgm1_metadata/gdalinfo
: > /work/dgm1_metadata/sha256.txt
while IFS= read -r name; do
    test -n "$name"
    source="/work/dgm1_sources/$name"
    test -s "$source"
    gdalinfo -json "$source" > "/work/dgm1_metadata/gdalinfo/$name.json.tmp"
    mv "/work/dgm1_metadata/gdalinfo/$name.json.tmp" "/work/dgm1_metadata/gdalinfo/$name.json"
    sha256sum "$source" >> /work/dgm1_metadata/sha256.txt
done < /work/dgm1_metadata/source_names.txt
""".strip()


def parse_metalink(meta4_path: str | Path) -> list[dict]:
    path = Path(meta4_path)
    if not path.is_file():
        raise FileNotFoundError(f"DGM1 Metalink does not exist: {path}")

    root = ET.parse(path).getroot()
    sources = []
    seen_names = set()
    seen_urls = set()
    for file_element in root.iter():
        if _local_name(file_element.tag) != "file":
            continue
        name = file_element.get("name", "")
        if not SOURCE_NAME_PATTERN.fullmatch(name) or Path(name).name != name:
            raise ValueError(f"Unsafe or unexpected DGM1 filename in Metalink: {name!r}")
        if name in seen_names:
            raise ValueError(f"Duplicate DGM1 file entry in Metalink: {name}")

        urls = []
        for child in file_element:
            if _local_name(child.tag) != "url" or not child.text:
                continue
            url = child.text.strip()
            parsed = urlparse(url)
            if parsed.scheme != "https" or Path(parsed.path).name != name:
                raise ValueError(f"Invalid URL for {name}: {url}")
            if url not in urls:
                urls.append(url)
        if not urls:
            raise ValueError(f"No HTTPS download URL found for {name}")

        unique_urls = [url for url in urls if url not in seen_urls]
        if not unique_urls:
            raise ValueError(f"All URLs for {name} duplicate another Metalink file")
        seen_names.add(name)
        seen_urls.update(unique_urls)
        sources.append({"name": name, "urls": unique_urls})

    if len(sources) != EXPECTED_SOURCE_COUNT:
        raise ValueError(
            f"Expected {EXPECTED_SOURCE_COUNT} unique DGM1 files, found {len(sources)}"
        )
    return sources


def _parse_metalink_callable(run_id, task_id):
    job_dir = Path(get_job_dir(run_id))
    mf.update_step(str(job_dir), task_id, "running")
    try:
        sources = parse_metalink(DGM1_META4_PATH)
        metadata_dir = job_dir / "dgm1_metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        _write_json(
            metadata_dir / "sources.json",
            {
                "source": str(DGM1_META4_PATH),
                "file_count": len(sources),
                "url_count": sum(len(item["urls"]) for item in sources),
                "sources": sources,
            },
        )
        with (metadata_dir / "source_names.txt").open("w", encoding="utf-8") as file:
            for item in sources:
                file.write(f"{item['name']}\n")
        with (metadata_dir / "source_paths.txt").open("w", encoding="utf-8") as file:
            for item in sources:
                file.write(f"/work/dgm1_sources/{item['name']}\n")
        mf.update_step(str(job_dir), task_id, "success")
    except Exception as error:
        mf.update_step(str(job_dir), task_id, "failed", error=str(error))
        raise


def _download_sources_callable(run_id, task_id):
    job_dir = Path(get_job_dir(run_id))
    mf.update_step(str(job_dir), task_id, "running")
    try:
        with (job_dir / "dgm1_metadata" / "sources.json").open(encoding="utf-8") as file:
            sources = json.load(file)["sources"]
        destination = job_dir / "dgm1_sources"
        destination.mkdir(parents=True, exist_ok=True)
        worker_count = max(1, min(DGM1_DOWNLOAD_WORKERS, len(sources)))
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(_download_source, source, destination): source["name"]
                for source in sources
            }
            for future in concurrent.futures.as_completed(futures):
                name = futures[future]
                result = future.result()
                results.append(result)
                print(f"{result['status']}: {name} ({result['bytes']} bytes)")
        results.sort(key=lambda item: item["name"])
        _write_json(
            job_dir / "dgm1_metadata" / "downloads.json",
            {"worker_count": worker_count, "files": results},
        )
        mf.update_step(str(job_dir), task_id, "success")
    except Exception as error:
        mf.update_step(str(job_dir), task_id, "failed", error=str(error))
        raise


def _download_source(source: dict, destination: Path) -> dict:
    final_path = destination / source["name"]
    partial_path = destination / f"{source['name']}.part"
    if final_path.is_file() and final_path.stat().st_size > 0:
        return {
            "name": source["name"],
            "url": source["urls"][0],
            "bytes": final_path.stat().st_size,
            "status": "reused",
        }

    last_error = None
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        for url in source["urls"]:
            try:
                _download_url(url, partial_path)
                if not partial_path.is_file() or partial_path.stat().st_size == 0:
                    raise IOError(f"Downloaded an empty file from {url}")
                os.replace(partial_path, final_path)
                return {
                    "name": source["name"],
                    "url": url,
                    "bytes": final_path.stat().st_size,
                    "status": "downloaded",
                }
            except Exception as error:
                last_error = error
                print(f"Download attempt {attempt} failed for {source['name']}: {error}")
        if attempt < DOWNLOAD_ATTEMPTS:
            time.sleep(min(2 ** (attempt - 1), 8))
    raise RuntimeError(
        f"Failed to download {source['name']} after {DOWNLOAD_ATTEMPTS} attempts"
    ) from last_error


def _download_url(url: str, partial_path: Path) -> None:
    offset = partial_path.stat().st_size if partial_path.exists() else 0
    headers = {"User-Agent": "Regensburg-DGM1-Airflow/1.0"}
    if offset:
        headers["Range"] = f"bytes={offset}-"
    request = urllib.request.Request(url, headers=headers)
    try:
        response = urllib.request.urlopen(request, timeout=120)
    except urllib.error.HTTPError as error:
        if error.code == 416 and offset:
            partial_path.unlink(missing_ok=True)
            return _download_url(url, partial_path)
        raise

    with response:
        status = getattr(response, "status", response.getcode())
        append = offset > 0 and status == 206
        if append:
            content_range = response.headers.get("Content-Range", "")
            if not content_range.startswith(f"bytes {offset}-"):
                raise IOError(f"Unexpected Content-Range while resuming {url}: {content_range}")
        mode = "ab" if append else "wb"
        expected = response.headers.get("Content-Length")
        expected_size = (offset if append else 0) + int(expected) if expected else None
        with partial_path.open(mode) as file:
            shutil.copyfileobj(response, file, length=DOWNLOAD_CHUNK_SIZE)
    if expected_size is not None and partial_path.stat().st_size != expected_size:
        raise IOError(
            f"Incomplete response for {url}: expected {expected_size} bytes, "
            f"received {partial_path.stat().st_size}"
        )


def _validate_source_reports_callable(run_id, task_id):
    job_dir = Path(get_job_dir(run_id))
    mf.update_step(str(job_dir), task_id, "running")
    try:
        metadata_dir = job_dir / "dgm1_metadata"
        with (metadata_dir / "sources.json").open(encoding="utf-8") as file:
            sources = json.load(file)["sources"]
        hashes = _read_hashes(metadata_dir / "sha256.txt")
        expected_names = {source["name"] for source in sources}
        if set(hashes) != expected_names:
            raise ValueError("SHA-256 report does not cover exactly the Metalink sources")

        bounds = [math.inf, math.inf, -math.inf, -math.inf]
        bounds_wgs84 = [math.inf, math.inf, -math.inf, -math.inf]
        files = []
        for source in sources:
            name = source["name"]
            with (metadata_dir / "gdalinfo" / f"{name}.json").open(encoding="utf-8") as file:
                report = json.load(file)
            _validate_gdal_report(name, report)
            corners = report["cornerCoordinates"]
            xs = [point[0] for point in corners.values() if isinstance(point, list)]
            ys = [point[1] for point in corners.values() if isinstance(point, list)]
            bounds = [
                min(bounds[0], *xs),
                min(bounds[1], *ys),
                max(bounds[2], *xs),
                max(bounds[3], *ys),
            ]
            wgs84_ring = report.get("wgs84Extent", {}).get("coordinates", [[]])[0]
            if not wgs84_ring:
                raise ValueError(f"{name} has no WGS84 extent in its GDAL report")
            longitudes = [point[0] for point in wgs84_ring]
            latitudes = [point[1] for point in wgs84_ring]
            bounds_wgs84 = [
                min(bounds_wgs84[0], *longitudes),
                min(bounds_wgs84[1], *latitudes),
                max(bounds_wgs84[2], *longitudes),
                max(bounds_wgs84[3], *latitudes),
            ]
            files.append(
                {
                    "name": name,
                    "sha256": hashes[name],
                    "size": report["size"],
                    "geo_transform": report["geoTransform"],
                }
            )

        _write_json(
            metadata_dir / "source_validation.json",
            {
                "valid": True,
                "file_count": len(files),
                "horizontal_crs": "EPSG:25832",
                "resolution_metres": 1.0,
                "vertical_datum": {
                    "name": "DHHN2016",
                    "source": "dataset documentation; not encoded in the GeoTIFF CRS",
                    "transformation_applied": False,
                },
                "bounds_epsg_25832": bounds,
                "bounds_wgs84": bounds_wgs84,
                "files": files,
            },
        )
        mf.update_step(str(job_dir), task_id, "success")
    except Exception as error:
        mf.update_step(str(job_dir), task_id, "failed", error=str(error))
        raise


def _validate_gdal_report(name: str, report: dict) -> None:
    if report.get("driverShortName") != "GTiff":
        raise ValueError(f"{name} is not a GeoTIFF")
    if report.get("size") != [1000, 1000]:
        raise ValueError(f"{name} has unexpected raster dimensions: {report.get('size')}")
    epsg = report.get("stac", {}).get("proj:epsg")
    wkt = report.get("coordinateSystem", {}).get("wkt", "")
    if epsg != 25832 and 'ID["EPSG",25832]' not in wkt:
        raise ValueError(f"{name} is not EPSG:25832")
    transform = report.get("geoTransform")
    if (
        not isinstance(transform, list)
        or len(transform) != 6
        or not math.isclose(transform[1], 1.0, abs_tol=1e-9)
        or not math.isclose(transform[5], -1.0, abs_tol=1e-9)
        or not math.isclose(transform[2], 0.0, abs_tol=1e-9)
        or not math.isclose(transform[4], 0.0, abs_tol=1e-9)
    ):
        raise ValueError(f"{name} is not an unrotated 1 m raster")
    bands = report.get("bands", [])
    if len(bands) != 1 or bands[0].get("type") != "Float32":
        raise ValueError(f"{name} does not have one Float32 elevation band")
    if bands[0].get("noDataValue") != -9999.0:
        raise ValueError(f"{name} has unexpected NoData value: {bands[0].get('noDataValue')}")


def _read_hashes(path: Path) -> dict[str, str]:
    hashes = {}
    with path.open(encoding="utf-8") as file:
        for line in file:
            digest, source_path = line.rstrip("\n").split(maxsplit=1)
            name = Path(source_path.strip()).name
            if not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ValueError(f"Invalid SHA-256 digest for {name}")
            if name in hashes:
                raise ValueError(f"Duplicate SHA-256 record for {name}")
            hashes[name] = digest
    return hashes


def make_parse_metalink_task() -> PythonOperator:
    return PythonOperator(
        task_id="parse_metalink",
        python_callable=_parse_metalink_callable,
        op_kwargs={"task_id": "parse_metalink"},
    )


def make_download_sources_task() -> PythonOperator:
    return PythonOperator(
        task_id="download_sources",
        python_callable=_download_sources_callable,
        op_kwargs={"task_id": "download_sources"},
        retries=2,
        retry_delay=timedelta(minutes=2),
    )


def make_inspect_sources_task() -> BaseOperator:
    return JobDockerOperator(
        task_id="inspect_sources",
        image=GDAL_IMAGE,
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        user=f"{os.getuid()}:{os.getgid()}",
        docker_url=DOCKER_HOST,
        command=["sh", "-ec", GDAL_INSPECTION_COMMAND],
    )


def make_validate_source_reports_task() -> PythonOperator:
    return PythonOperator(
        task_id="validate_source_reports",
        python_callable=_validate_source_reports_callable,
        op_kwargs={"task_id": "validate_source_reports"},
    )


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _write_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    with temporary.open("w", encoding="utf-8") as file:
        json.dump(value, file, indent=2)
        file.write("\n")
    os.replace(temporary, path)
