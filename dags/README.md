# Regensburg DAG bundle

This directory is one self-contained Airflow DAG bundle:

```text
dags/
├── digital_twin_pipeline.py
├── dgm1_terrain_pipeline.py
├── pipeline/                 # Shared operators, tasks, and configuration
├── dgm1.meta4                # DGM1 source Metalink
├── requirements.txt
├── README.md
└── .airflowignore
```

`digital_twin_pipeline` converts a CityGML ZIP from S3 into enriched CityGML,
3D Tiles, and an address database. `dgm1_terrain_pipeline` downloads the DGM1
sources listed in the Metalink and publishes a Cesium Quantized Mesh terrain
tileset. Both DAGs are triggered manually.

## Airflow environment and dependencies

The project is currently tested with Airflow 3.1.7 and Python 3.12 on Linux.
Install [requirements.txt](requirements.txt) in the environments that parse
and execute the DAGs. It declares the Amazon, Docker, and Standard providers,
plus the Docker SDK imported by the shared operator. Processing tools such as
GDAL, CTB, and the CityJSON converters run in their own Docker images.

Install `apache-airflow-providers-git` in the Airflow deployment to use
`GitDagBundle`, and make the Git executable available there. Fetching a bundle
does not install its Python requirements; install these when building or
provisioning the Airflow environment.

For example, from the bundle directory (`dags/` in the development repository,
or the published repository root), using the project's current runtime:

```bash
AIRFLOW_VERSION=3.1.7
PYTHON_VERSION=3.12
python -m pip install "apache-airflow==${AIRFLOW_VERSION}" \
  apache-airflow-providers-git \
  -r requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

Use the Airflow and Python versions of your target deployment for these values.
Keeping Airflow explicitly pinned in the install command prevents dependency
resolution from upgrading the deployment's Airflow version.

## Git bundle configuration

Configure `AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST` in the Airflow
deployment, replacing the repository URL and tracking branch:

```bash
export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
  {
    "name": "regensburg",
    "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
    "kwargs": {
      "repo_url": "https://github.com/csi-FOXBYTE/Regensburg_DigitalerEnergieZwilling_DAGs.git",
      "tracking_ref": "main"
    }
  }
]'
```

This selects both DAGs and their shared files from the same Git revision.
The published repository root is the bundle, so no `subdir` is needed. To use the
development repository instead, change `repo_url` and add `"subdir": "dags"`.
For private repositories, configure authentication through the Git provider's
Airflow connection. See the [Git bundle documentation](https://airflow.apache.org/docs/apache-airflow-providers-git/stable/bundles/index.html).

The default Metalink path resolves relative to this bundle's code, so it works
in versioned checkouts without depending on the original repository location.

## Runtime prerequisites

- Workers need access to a Docker daemon and permission to pull the processing
  images declared in [pipeline/config.py](pipeline/config.py).
- `CITYJSON_WORK_DIR` must be writable by the Airflow task user and visible at
  the same absolute path to every worker running this bundle and to the Docker
  daemon. Each Docker task bind-mounts its run directory at `/work`. For workers
  running in containers, mount this shared directory at the same path inside
  the workers and on the Docker host. Processing containers use the worker's
  UID and GID.
- Keep the workspace outside the Git checkout so bundle refreshes and cleanup
  cannot remove run data. Provide sufficient storage for the DGM1 source TIFFs
  and generated terrain.
- Workers need network access to S3 and the HTTPS sources in `dgm1.meta4`.
  Output buckets must already exist. Use dedicated output buckets: the DAGs
  clear them before publishing replacements.

## Environment variables

Set these in the Airflow processes that parse and execute the DAGs. The bundle
does not load a `.env` file itself. Configuration values are read when modules
are imported; restart the affected processes after changing their environment.

### Shared configuration

| Variable | Default / requirement | Purpose |
|---|---|---|
| `AIRFLOW_CONN_DET_RG_S3` | Required unless the `det_rg_s3` connection is configured in Airflow or a secrets backend | Airflow connection of type `aws` used by both DAGs. Contains S3 credentials, region, and any custom endpoint. |
| `CITYJSON_WORK_DIR` | `/opt/airflow/data/cityjson-to-3d-tiles` | Absolute shared workspace path; configure it explicitly for your deployment. Runs write under `jobs/<run-id>/`. |
| `DOCKER_HOST` | `unix://var/run/docker.sock` | Docker daemon endpoint used by processing tasks. |

Example environment values (replace the paths, endpoint, and credentials):

```bash
export CITYJSON_WORK_DIR=/srv/regensburg/workdir
export DOCKER_HOST=unix:///var/run/docker.sock
export AIRFLOW_CONN_DET_RG_S3='{"conn_type":"aws","login":"your-access-key","password":"your-secret-key","extra":{"endpoint_url":"https://your-s3-host","region_name":"eu-central-1","config_kwargs":{"s3":{"addressing_style":"path"}}}}'
```

Keep the connection JSON on one line. Both DAGs explicitly request `det_rg_s3`
and fail if it is missing or has the wrong connection type. Supply credentials
appropriate to your S3 service through that connection or its configured AWS
authentication mechanism.

### CityJSON to 3D Tiles options

These optional values are forwarded to the converter container.

| Variable | Default | Purpose |
|---|---|---|
| `CITYJSON_APPEARANCE` | `rgbTexture` | Converter appearance mode. |
| `CITYJSON_THREAD_COUNT` | `4` | Converter thread count. |
| `CITYJSON_HAS_ALPHA_ENABLED` | `true` | Enable alpha handling. |
| `CITYJSON_SIMPLIFY_ADDRESSES` | `false` | Enable address simplification. |
| `CITYJSON_SEMANTIC_SURFACE_COLORS` | `{"RoofSurface":"#e30613"}` | JSON mapping of semantic surface types to colors. |
| `CITYJSON_SHOW_STACK_TRACE` | `false` | Include converter stack traces. |

### DGM1 terrain options

| Variable | Default | Purpose |
|---|---|---|
| `DGM1_META4_PATH` | `dgm1.meta4` in this bundle | Optional override for the source Metalink. Use an absolute path readable by the worker. The current pipeline expects 368 unique source TIFFs. |
| `DGM1_DOWNLOAD_WORKERS` | `4` | Concurrent source downloads. |
| `DGM1_CTB_THREADS` | `4` | Terrain generation threads. |
| `DGM1_UPLOAD_WORKERS` | `8` | Concurrent terrain uploads. |

Use positive integers for concurrency settings. Leave `DGM1_META4_PATH` unset
to use the Metalink shipped with the selected bundle revision.

## Trigger parameters

Supply these through the Airflow trigger form or API.

| DAG | Required parameters | Optional parameters |
|---|---|---|
| `digital_twin_pipeline` | `bucket`, `key`, `tiles_output_bucket`, `gml_output_bucket` | `source_crs`, `municipality_key`, `age_zones_key`, `geothermal_key`, `skip_cleanup` |
| `dgm1_terrain_pipeline` | `terrain_output_bucket` | `skip_cleanup` |

`bucket` and `key` select the input CityGML ZIP. Optional GeoPackage keys refer
to objects in that same input bucket. The municipality defaults to Regensburg
(`09362000`). The address database is uploaded to `tiles_output_bucket`.

Successful runs remove intermediate files unless `skip_cleanup` is `true`.
Failed runs retain their workspace. The run's `manifest.json` records progress
and retained artifacts.

## DGM1 attribution

The DGM1 data is provided by Bayerische Vermessungsverwaltung
(www.geodaten.bayern.de) under CC BY 4.0. The terrain pipeline embeds this
attribution in `layer.json` and preserves DHHN2016 heights without a vertical
transformation. Project code is licensed under LGPL-3.0-or-later.
