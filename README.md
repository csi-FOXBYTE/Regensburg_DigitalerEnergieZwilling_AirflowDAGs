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
