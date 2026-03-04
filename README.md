# Airflow DAG Environment

This repository now contains a default Docker-based Apache Airflow setup and your DAG:

- `dags/cityjson_to_3d_tiles.py`
- `dags/citygml_zip_to_storage.py`

## Prerequisites

- Docker Desktop (or Docker Engine) with Compose support
- Access to Docker socket (`/var/run/docker.sock`) from Airflow containers

## First start

1. Create the environment file:
   ```powershell
   Copy-Item .env.example .env
   ```
2. Edit `.env` and set `CITYJSON_WORK_DIR` to an **absolute host path** that Docker can mount.
   - If needed on Windows, set `DOCKER_SOCKET_PATH=//var/run/docker.sock`.
   - `CITYJSON_WORK_DIR` may be `C:/...`; the DAG normalizes Windows drive paths to `/mnt/<drive>/...` for Docker Desktop.
3. Create input/output directory (example):
   ```powershell
   New-Item -ItemType Directory -Force -Path .\data\cityjson-to-3d-tiles | Out-Null
   ```
4. Initialize Airflow metadata DB and admin user:
   ```powershell
   docker compose up airflow-init
   ```
5. Start Airflow:
   ```powershell
   docker compose up -d airflow-webserver airflow-scheduler
   ```
   This also starts LocalStack S3 and creates the configured test bucket.
6. Open UI: <http://localhost:8080>
   - Username: value of `_AIRFLOW_WWW_USER_USERNAME` (default `airflow`)
   - Password: value of `_AIRFLOW_WWW_USER_PASSWORD` (default `airflow`)

## Local testing (this repo)

1. Validate compose setup:
   ```powershell
   docker compose config
   ```
2. (Optional) Pull processing image:
   ```powershell
   docker pull ghcr.io/csi-foxbyte/cityjson-to-3d-tiles:latest
   ```
3. Ensure your host work directory exists:
   ```powershell
   New-Item -ItemType Directory -Force -Path .\data\cityjson-to-3d-tiles | Out-Null
   ```
4. Start or restart the Airflow services:
   ```powershell
   docker compose up -d airflow-webserver airflow-scheduler
   ```
5. Trigger the DAG:
   ```powershell
   docker compose exec airflow-webserver airflow dags trigger cityjson_to_3d_tiles
   ```
6. Check DAG and task state:
   ```powershell
   docker compose exec airflow-webserver airflow dags list-runs -d cityjson_to_3d_tiles
   docker compose exec airflow-webserver airflow tasks states-for-dag-run cityjson_to_3d_tiles <dag_run_id>
   ```
7. Follow scheduler logs while running:
   ```powershell
   docker compose logs -f airflow-scheduler
   ```
8. Verify outputs in your `CITYJSON_WORK_DIR` host folder (mounted to `/work` in the container).

## Debugging

Use these commands for focused task-level debugging instead of full DAG runs.

1. Start Airflow services:
   ```powershell
   docker compose up -d airflow-webserver airflow-scheduler
   ```
2. Test a single task for one logical date:
   ```powershell
   docker compose exec airflow-webserver airflow tasks test citygml_zip_to_storage extract_citygml_zip 2026-03-04
   ```
3. Watch scheduler logs in parallel:
   ```powershell
   docker compose logs -f airflow-scheduler
   ```
4. Add task code logs for variable/state inspection:
   ```python
   import logging
   log = logging.getLogger(__name__)
   log.info("Value X=%s", x)
   ```
5. Use Python debugger for step-by-step inspection:
   ```python
   import pdb; pdb.set_trace()
   ```

### Debugging DockerOperator issues

To isolate Airflow from container execution issues, run the same container command directly:

```powershell
docker run --rm `
  -v "<ABS_WORK_DIR>:/work" `
  -e APPEARANCE=rgbTexture `
  ghcr.io/csi-foxbyte/cityjson-to-3d-tiles:latest
```

If `docker run` fails, fix image/mount/env first. If it succeeds but Airflow task fails, inspect Airflow task logs and operator parameters.

## LocalStack S3 simulation (external source)

The compose setup includes LocalStack with only S3 enabled.

- Container endpoint (for DAG code running in Airflow containers): `http://localstack:4566`
- Host endpoint (for tools on your machine): `http://localhost:4566`
- Bucket name: value of `S3_BUCKET` in `.env` (default `external-downloads`)
- Preconfigured S3 GUI: `http://localhost:3000` (or `S3_GUI_PORT` from `.env`)

### Verify S3 is up and bucket exists

```powershell
docker compose up -d localstack localstack-s3-init
docker compose exec localstack awslocal s3 ls
```

### Open the S3 GUI

```powershell
docker compose up -d s3-gui
```

Then open:

- <http://localhost:3000> (or your configured `S3_GUI_PORT`)

The GUI is preconfigured for:

- endpoint `http://localstack:4566`
- bucket `S3_BUCKET`
- credentials `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY`
- path-style addressing (required for LocalStack)

### Upload a sample object for download testing

```powershell
docker compose exec localstack sh -c "echo 'demo-file' > /tmp/sample.txt && awslocal s3 cp /tmp/sample.txt s3://${S3_BUCKET:-external-downloads}/sample.txt"
docker compose exec localstack awslocal s3 ls s3://${S3_BUCKET:-external-downloads}
```

### Environment variables for DAG implementation

Set in `.env` (see `.env.example`):

- `S3_ENDPOINT_URL` (default `http://localstack:4566`)
- `S3_REGION` (default `eu-central-1`)
- `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` (default `test` / `test`)
- `S3_BUCKET` (default `external-downloads`)
- `S3_GUI_PORT` (default `3000`)

## Multi-step ZIP pipeline

Use DAG `citygml_zip_to_storage` for a full chain:

1. Extract uploaded ZIP with CityGML files.
2. Generate CityJSON + 3D Tiles.
3. Copy resulting tiles to a storage target folder.

### Prepare directories

Ensure these directories exist on host:

- `CITYGML_UPLOAD_DIR` (input ZIPs)
- `CITYJSON_WORK_DIR` (working dir for conversion)
- `CITYJSON_STORAGE_DIR` (final copied outputs)

Example:
```powershell
New-Item -ItemType Directory -Force -Path .\data\uploads | Out-Null
New-Item -ItemType Directory -Force -Path .\data\cityjson-to-3d-tiles | Out-Null
New-Item -ItemType Directory -Force -Path .\data\storage | Out-Null
```

### Trigger from Airflow UI

Airflow's default UI does not provide a native binary file upload control for DAG triggers.
Use the configured upload folder for ZIP files, then trigger with run config.

1. Copy ZIP file(s) into `CITYGML_UPLOAD_DIR` (for local default: `data/uploads`).
2. In Airflow UI open DAG `citygml_zip_to_storage`.
3. Trigger with optional run config:
```json
{
  "zip_file": "my-citygml-batch.zip",
  "storage_subdir": "batch_001"
}
```

- `zip_file` optional: if omitted, newest ZIP in upload folder is used.
- `storage_subdir` optional: if omitted, Airflow `run_id` is used.

## Stop and reset

- Stop containers:
  ```powershell
  docker compose down
  ```
- Full reset including metadata database volume:
  ```powershell
  docker compose down -v
  ```

## Notes for DockerOperator

- The DAG uses `DockerOperator`, so the scheduler container needs access to Docker socket.
- Socket mount path is controlled by `DOCKER_SOCKET_PATH` in `.env`.
- Socket group mapping is controlled by `DOCKER_GID` in `.env` (default `101`).
- The value in `CITYJSON_WORK_DIR` must be a host path visible to the Docker daemon.
- The value in `CITYGML_UPLOAD_DIR` must be a host path visible to the Docker daemon.
- The value in `CITYJSON_STORAGE_DIR` must be a host path visible to the Docker daemon.
- Files should be placed in that directory before triggering the DAG.
- For deeper tool errors, set `CITYJSON_SHOW_STACK_TRACE=true` in `.env`.

## Troubleshooting

- `Permission denied: /opt/airflow/logs/...`:
  - Logs are stored in Docker volume `airflow-logs-volume` to avoid host bind permission issues on Windows.
  - If you still see stale state, run:
    ```powershell
    docker compose down -v
    ```
- `Permission denied` on `/var/run/docker.sock`:
  - Set `DOCKER_GID` in `.env` to the group ID of the mounted socket.
  - In this setup the socket group is `101` (seen in container via `ls -l /var/run/docker.sock`), which is already the default.
  - Recreate containers after changing it:
    ```powershell
    docker compose up -d --force-recreate airflow-webserver airflow-scheduler
    ```
- `invalid mount config ... mount path must be absolute` with source `C:/...`:
  - Use the latest DAG version in this repo (it normalizes `C:/...` to `/mnt/c/...`).
  - Recreate services so scheduler loads updated DAG:
    ```powershell
    docker compose up -d --force-recreate airflow-webserver airflow-scheduler
    ```
