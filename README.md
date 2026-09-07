# Airflow DAG Environment

Apache Airflow runs **locally** (not in Docker). Only the processing tasks run inside Docker containers via `DockerOperator`. The `docker-compose.yaml` starts supporting services only: **LocalStack S3** and the **S3 GUI**.

## Prerequisites

- Linux or WSL on Windows
- Docker Engine with Compose support (for tasks and LocalStack)
- Python 3.12

## First start

1. Run the init script to create the venv and install Airflow:
   ```bash
   ./init.sh
   ```

2. Copy the environment file and set `CITYJSON_WORK_DIR` to an absolute host path:
   ```bash
   cp .env.example .env
   ```

## Running

In VSCode a new terminal activates the venv and exports `.env` automatically via `start.sh`. Otherwise source it manually:
```bash
source start.sh
```

Start LocalStack S3:
```bash
docker compose up -d
```

Start Airflow:
```bash
airflow standalone
```

The S3 clients are configured through the project-specific Airflow connection
environment variable `AIRFLOW_CONN_DET_RG_S3` from `.env`. Both DAGs use this
explicit connection while selecting their own buckets through DAG parameters.

Each value is an Airflow AWS connection encoded as one line of JSON. The DAGs
validate their named connection before creating an S3 client, so a missing or
mistyped connection does not fall back to process-wide `AWS_*` credentials.

- Airflow UI: <http://localhost:8080> — username `admin`, password in `.airflow/simple_auth_manager_passwords.json`
- S3 GUI: <http://localhost:3000>

## DAG: `digital_twin_pipeline`

Processes a CityGML ZIP from S3 through the full pipeline and uploads results back to S3.

**Pipeline:**

1. `ensure_dirs` → `download` → `extract_zip` → `gml_to_cityjson` → `enrich`
2. From `enrich` (parallel):
   - `json_to_3dtiles` → `upload_tiles`
   - `json_to_citygml` → `clear_gml_bucket` → `upload_gml`
3. `cleanup` (runs after both uploads)

**Trigger parameters:**

| Parameter | Description |
|---|---|
| `bucket` | S3 bucket containing the input ZIP |
| `key` | Key (path) of the ZIP in the bucket |
| `tiles_output_bucket` | S3 bucket for 3D Tiles output |
| `gml_output_bucket` | Dedicated S3 bucket whose contents are replaced by the CityGML output |
| `source_crs` | Source CRS (default: UTM zone 32 / GRS80) |
| `municipality_key` | Municipality key used to restrict enrichment to one city (default: `09362000`, Regensburg) |
| `age_zones_key` | Optional key of the Baualtersklassen GeoPackage in the input bucket |
| `geothermal_key` | Optional key of the geothermal GeoPackage in the input bucket |
| `skip_cleanup` | Keep the complete run workspace after successful uploads for debugging (default: `false`) |

To do a test run, upload a ZIP to S3 via the S3 GUI. To include the
optional enrichment datasets, also upload `test_data/Baualtersklassen.gpkg`
and/or `test_data/Geothermie.gpkg` and set their object keys in
`age_zones_key` and `geothermal_key`. Then trigger the DAG from the Airflow UI.
Set `skip_cleanup` to `true` to retain the downloaded inputs and generated
artifacts after a successful run. Failed runs retain their artifacts regardless.
The GML output bucket is cleared only after CityGML conversion succeeds and
immediately before the generated files are uploaded.

## DAG: `dgm1_terrain_pipeline`

Converts all 368 DGM1 GeoTIFFs referenced by the repository-root
`dgm1.meta4` into an EPSG:4326 Cesium Quantized Mesh terrain tileset at zoom
levels 0 through 18.

The full-only pipeline:

1. Parses the Metalink and removes its repeated URLs.
2. Downloads the TIFFs with bounded concurrency, retries, and `.part` file
   resume support.
3. Uses digest-pinned GDAL 3.13.2 Docker tasks to inspect every TIFF, calculate
   SHA-256 hashes, and build a VRT mosaic.
4. Uses digest-pinned CTB 0.4.1 to generate Cesium-friendly Quantized Mesh
   terrain with oct-encoded vertex normals.
5. Normalizes gzip-compressed CTB output to raw `.terrain`, if necessary.
6. Validates zoom coverage, representative low/middle/high tiles, Quantized
   Mesh structure, normal extensions, and the local HTTP loading contract used
   by `CesiumTerrainProvider.fromUrl(...)`.
7. Clears the dedicated output bucket, uploads every terrain tile, and uploads
   `layer.json` last.

The VRT references the source files and avoids creating another large raster
mosaic. CTB performs the horizontal transformation from EPSG:25832 to its
EPSG:4326 geodetic terrain profile. Heights are treated as DHHN2016 and are not
vertically transformed.

Trigger parameters:

| Parameter | Description |
|---|---|
| `terrain_output_bucket` | Dedicated S3 bucket whose contents are replaced by the validated terrain tileset |
| `skip_cleanup` | Keep the complete run workspace after successful publication for debugging (default: `false`) |

The output bucket must already exist. It is cleared only after local validation
passes. `layer.json` is uploaded last so a new Cesium client cannot discover a
partially uploaded replacement. Failed runs always retain their workspace.
Successful runs retain it when `skip_cleanup` is `true`.

Large source lists, hashes, and raster data are stored in the run workspace,
not XCom. Concurrency can be adjusted with `DGM1_DOWNLOAD_WORKERS`,
`DGM1_CTB_THREADS`, and `DGM1_UPLOAD_WORKERS`.

Data attribution embedded in `layer.json`:

> Bayerische Vermessungsverwaltung – www.geodaten.bayern.de
>
> Source DGM1 data converted to Cesium Quantized Mesh; DHHN2016 heights
> preserved without vertical transformation.

The Bavarian DGM1 source data is provided under CC BY 4.0. The project remains
licensed under LGPL-3.0-or-later; GDAL and CTB retain their respective upstream
licenses.

## Software bill of materials

The checked-in [`SBOM.cdx.json`](SBOM.cdx.json) is a CycloneDX 1.6 inventory;
[`SBOM.csv`](SBOM.csv) provides the same components in a review-friendly table.
Generate both files with the Python tooling in an isolated environment:

```bash
python3 -m venv .sbom-env
.sbom-env/bin/python -m pip install -r requirements-sbom.txt
.sbom-env/bin/python scripts/generate_sbom.py
```

By default the generator reads package metadata from the installed `.airflow-env`.
Run `./init.sh` first, or pass `--python /path/to/python` to use another equivalent
Airflow environment. Only the dependency closure of the packages imported by the
two DAGs is retained; local development services and the S3 GUI are excluded. The
generator also adds the pipeline container images declared in `sbom.config.json`.
Container contents are not expanded; keep `sbom.config.json` synchronized with
`dags/pipeline/config.py`.

## LocalStack S3

- Host endpoint: `http://localhost:4566`
- Default bucket: `external-downloads` (configurable via `S3_BUCKET` in `.env`)
- S3 GUI: <http://localhost:3000>

For LocalStack, point `AIRFLOW_CONN_DET_RG_S3` at `http://localhost:4566`. The
supporting containers use the separately scoped `LOCALSTACK_S3_*` variables,
while the GUI uses `S3_GUI_*`; none of these are used implicitly by the DAGs.

## Debugging

Test a single task:
```bash
airflow tasks test digital_twin_pipeline download_file_task 2026-01-01
```

For `DockerOperator` issues, run the container directly to isolate from Airflow:
```bash
docker run --rm \
  -v "<ABS_WORK_DIR>:/work" \
  -e INPUT_DIR=/work/json \
  -e OUTPUT_DIR=/work/3d_tiles \
  ghcr.io/csi-foxbyte/cityjson-to-3d-tiles:latest
```

## Stop / Reset

```bash
# Stop LocalStack
docker compose down

# Full reset including S3 data
docker compose down -v
```
