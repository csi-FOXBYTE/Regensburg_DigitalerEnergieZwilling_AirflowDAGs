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

- Airflow UI: <http://localhost:8080> — username `admin`, password in `.airflow/simple_auth_manager_passwords.json`
- S3 GUI: <http://localhost:3000>

## DAG: `digital_twin_pipeline`

Processes a CityGML ZIP from S3 through the full pipeline and uploads results back to S3.

**Pipeline:**

1. `ensure_dirs` → `download` → `extract_zip` → `gml_to_cityjson` → `enrich`
2. From `enrich` (parallel):
   - `json_to_3dtiles` → `upload_tiles`
   - `json_to_citygml` → `upload_gml`
3. `cleanup` (runs after both uploads)

**Trigger parameters:**

| Parameter | Description |
|---|---|
| `bucket` | S3 bucket containing the input ZIP |
| `key` | Key (path) of the ZIP in the bucket |
| `tiles_output_bucket` | S3 bucket for 3D Tiles output |
| `gml_output_bucket` | S3 bucket for CityGML output |
| `source_crs` | Source CRS (default: UTM zone 32 / GRS80) |

To do a test run: upload a ZIP to S3 via the S3 GUI, then trigger the DAG from the Airflow UI with the parameters above.

## LocalStack S3

- Host endpoint: `http://localhost:4566`
- Default bucket: `external-downloads` (configurable via `S3_BUCKET` in `.env`)
- S3 GUI: <http://localhost:3000>

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
