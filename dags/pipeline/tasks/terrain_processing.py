import gzip
import hashlib
import json
import os
import shutil
from pathlib import Path

from airflow.models import BaseOperator
from airflow.providers.standard.operators.python import PythonOperator

from pipeline import manifest as mf
from pipeline.config import (
    CTB_IMAGE,
    DGM1_CTB_THREADS,
    DOCKER_HOST,
    GDAL_IMAGE,
    get_job_dir,
)
from pipeline.operators.job_docker_operator import JobDockerOperator
from pipeline.terrain_validation import (
    ATTRIBUTION,
    GZIP_SIGNATURE,
    is_gzip_encoded,
    validate_tileset,
)


BUILD_VRT_COMMAND = """
set -eu
mkdir -p /work/dgm1_vrt /work/dgm1_metadata
gdalbuildvrt \
    -overwrite \
    -input_file_list /work/dgm1_metadata/source_paths.txt \
    /work/dgm1_vrt/dgm1.vrt
gdalinfo -json /work/dgm1_vrt/dgm1.vrt > /work/dgm1_metadata/vrt.gdalinfo.json.tmp
mv /work/dgm1_metadata/vrt.gdalinfo.json.tmp /work/dgm1_metadata/vrt.gdalinfo.json
""".strip()


CTB_COMMAND = f"""
set -eu
mkdir -p /work/terrain
ctb-tile \
    -f Mesh \
    -C \
    -N \
    -c {DGM1_CTB_THREADS} \
    -s 18 \
    -e 0 \
    -R \
    -o /work/terrain \
    /work/dgm1_vrt/dgm1.vrt
ctb-tile \
    -f Mesh \
    -C \
    -N \
    -c {DGM1_CTB_THREADS} \
    -s 18 \
    -e 0 \
    -R \
    -l \
    -o /work/terrain \
    /work/dgm1_vrt/dgm1.vrt
""".strip()


def _normalize_and_prepare_layer_callable(run_id, task_id):
    job_dir = Path(get_job_dir(run_id))
    terrain_dir = job_dir / "terrain"
    mf.update_step(str(job_dir), task_id, "running")
    try:
        terrain_paths = list(terrain_dir.rglob("*.terrain"))
        if not terrain_paths:
            raise FileNotFoundError(f"CTB produced no .terrain files in {terrain_dir}")
        compressed_count = 0
        for path in terrain_paths:
            with path.open("rb") as file:
                compressed = is_gzip_encoded(file.read(len(GZIP_SIGNATURE)))
            if not compressed:
                continue
            temporary = path.with_suffix(".terrain.raw")
            try:
                with gzip.open(path, "rb") as source, temporary.open("wb") as destination:
                    shutil.copyfileobj(source, destination, length=1024 * 1024)
                os.replace(temporary, path)
                compressed_count += 1
            finally:
                temporary.unlink(missing_ok=True)

        layer_path = terrain_dir / "layer.json"
        if not layer_path.is_file():
            raise FileNotFoundError(f"CTB did not generate {layer_path}")
        with layer_path.open(encoding="utf-8") as file:
            layer = json.load(file)
        with (job_dir / "dgm1_metadata" / "source_validation.json").open(
            encoding="utf-8"
        ) as file:
            source_validation = json.load(file)

        source_digest = _combined_source_digest(job_dir / "dgm1_metadata" / "sha256.txt")
        layer.pop("schema", None)
        layer.update(
            {
                "tilejson": "2.1.0",
                "version": source_digest[:16],
                "format": "quantized-mesh-1.0",
                "scheme": "tms",
                "extensions": ["octvertexnormals"],
                "tiles": ["{z}/{x}/{y}.terrain?v={version}"],
                "minzoom": 0,
                "maxzoom": 18,
                "projection": "EPSG:4326",
                "bbox": source_validation["bounds_wgs84"],
                "bounds": [-180.0, -90.0, 180.0, 90.0],
                "attribution": ATTRIBUTION,
                "description": (
                    "DGM1 terrain converted to Cesium Quantized Mesh; "
                    "horizontal source CRS EPSG:25832; DHHN2016 heights preserved "
                    "without vertical transformation."
                ),
                "sourceDataConversion": {
                    "converted": True,
                    "horizontalSourceCrs": "EPSG:25832",
                    "verticalDatum": "DHHN2016",
                    "verticalTransformationApplied": False,
                },
            }
        )
        _write_json(layer_path, layer)
        _write_json(
            job_dir / "dgm1_metadata" / "terrain_normalization.json",
            {
                "terrain_file_count": len(terrain_paths),
                "gzip_files_normalized": compressed_count,
                "output_encoding": "identity",
                "source_digest": source_digest,
            },
        )
        mf.update_step(str(job_dir), task_id, "success")
    except Exception as error:
        mf.update_step(str(job_dir), task_id, "failed", error=str(error))
        raise


def _validate_terrain_callable(run_id, task_id):
    job_dir = Path(get_job_dir(run_id))
    mf.update_step(str(job_dir), task_id, "running")
    try:
        report = validate_tileset(job_dir / "terrain")
        validation_dir = job_dir / "terrain_validation"
        validation_dir.mkdir(parents=True, exist_ok=True)
        _write_json(validation_dir / "report.json", {"valid": True, **report})
        mf.update_step(str(job_dir), task_id, "success")
    except Exception as error:
        mf.update_step(str(job_dir), task_id, "failed", error=str(error))
        raise


def make_build_vrt_task() -> BaseOperator:
    return JobDockerOperator(
        task_id="build_vrt",
        image=GDAL_IMAGE,
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        user=f"{os.getuid()}:{os.getgid()}",
        docker_url=DOCKER_HOST,
        command=["sh", "-ec", BUILD_VRT_COMMAND],
    )


def make_generate_terrain_task() -> BaseOperator:
    return JobDockerOperator(
        task_id="generate_terrain",
        image=CTB_IMAGE,
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        user=f"{os.getuid()}:{os.getgid()}",
        docker_url=DOCKER_HOST,
        command=["bash", "-ec", CTB_COMMAND],
    )


def make_normalize_and_prepare_layer_task() -> PythonOperator:
    return PythonOperator(
        task_id="normalize_and_prepare_layer",
        python_callable=_normalize_and_prepare_layer_callable,
        op_kwargs={"task_id": "normalize_and_prepare_layer"},
    )


def make_validate_terrain_task() -> PythonOperator:
    return PythonOperator(
        task_id="validate_terrain",
        python_callable=_validate_terrain_callable,
        op_kwargs={"task_id": "validate_terrain"},
    )


def _combined_source_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for line in sorted(file):
            digest.update(line)
    return digest.hexdigest()


def _write_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    with temporary.open("w", encoding="utf-8") as file:
        json.dump(value, file, indent=2)
        file.write("\n")
    os.replace(temporary, path)
