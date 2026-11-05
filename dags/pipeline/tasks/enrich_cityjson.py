import os
from datetime import timedelta
from airflow.models import BaseOperator
from pipeline.config import ENRICH_IMAGE, DOCKER_HOST, get_job_dir
from pipeline.operators.job_docker_operator import JobDockerOperator
from pipeline import manifest as mf


# Resource bounds for the enrichment container (untrusted CityJSON input).
# The per-file size/complexity limits are enforced by the tool itself; these
# bound the whole run so a crafted batch cannot run or allocate unbounded.
ENRICH_EXECUTION_TIMEOUT_MINUTES = int(os.getenv("ENRICH_EXECUTION_TIMEOUT_MINUTES", "120"))
ENRICH_MEM_LIMIT = os.getenv("ENRICH_MEM_LIMIT", "4g")
ENRICH_MAX_OLD_SPACE_MB = int(os.getenv("ENRICH_MAX_OLD_SPACE_MB", "3072"))

# Optional settings rendered as "" when their trigger parameter is unset. They
# are removed before the container starts, so the tool sees them as unset
# (an empty AGE_ZONES_FILE previously aborted the enrichment).
OPTIONAL_ENV_KEYS = ("SOURCE_CRS_FALLBACK", "AGE_ZONES_FILE", "GEOTHERMAL_FILE")


def drop_empty_optional_env(environment: dict) -> dict:
    return {
        key: value
        for key, value in environment.items()
        if key not in OPTIONAL_ENV_KEYS or str(value).strip()
    }


class EnrichDockerOperator(JobDockerOperator):
    """JobDockerOperator that omits optional settings whose template rendered
    empty and records an execution timeout as a failed step."""

    def execute(self, context):
        self.environment = drop_empty_optional_env(self.environment)
        try:
            return super().execute(context)
        except Exception:
            raise
        except BaseException as e:
            # AirflowTaskTimeout derives from BaseException and bypasses the
            # step bookkeeping of JobDockerOperator; without this the manifest
            # would keep the step "running" and finalize the run as success.
            mf.update_step(
                get_job_dir(context["run_id"]),
                self.task_id,
                "failed",
                error=f"{type(e).__name__}: {e}",
            )
            raise


def make_enrich_cityjson_task(
    src_dir: str,
    out_dir: str,
    address_db_dir: str,
    with_age_zones: bool = False,
    with_geothermal: bool = False,
) -> BaseOperator:
    environment = {
        "SOURCE_CRS_FALLBACK": "{{ params.source_crs if params.source_crs is not none else '' }}",
        "MUNICIPALITY_KEY": "{{ params.municipality_key }}",
        "ADJACENCY": "1",
        # Fail the task when any input file is skipped instead of publishing a
        # partial dataset.
        "STRICT": "1",
        "INPUT_DIR": f"/work/{src_dir}",
        "OUTPUT_DIR": f"/work/{out_dir}",
        "ADDRESS_OUTPUT": f"/work/{address_db_dir}/det-rg-addresses.sqlite",
        "NODE_OPTIONS": f"--max-old-space-size={ENRICH_MAX_OLD_SPACE_MB}",
    }
    if with_age_zones:
        environment["AGE_ZONES_FILE"] = "{{ '/work/gpkg/age_zones.gpkg' if params.get('age_zones_key') else '' }}"
    if with_geothermal:
        environment["GEOTHERMAL_FILE"] = "{{ '/work/gpkg/geothermal.gpkg' if params.get('geothermal_key') else '' }}"

    return EnrichDockerOperator(
        task_id="enrich_cityjson",
        image=ENRICH_IMAGE,
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        environment=environment,
        command="node dist/cli.mjs",
        user=f"{os.getuid()}:{os.getgid()}",
        docker_url=DOCKER_HOST,
        mem_limit=ENRICH_MEM_LIMIT,
        execution_timeout=timedelta(minutes=ENRICH_EXECUTION_TIMEOUT_MINUTES),
    )
