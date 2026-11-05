from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from pipeline.config import get_job_dir
from pipeline import manifest as mf


class JobDockerOperator(DockerOperator):
    """DockerOperator that mounts jobs/{job_id} as /work at runtime and tracks step status in the manifest.

    Processing containers run without a network (``network_mode="none"``) unless
    a task explicitly passes another mode: every converter reads its input from
    and writes its output to the mounted job directory, so a crafted input (for
    example a GDAL ``/vsicurl/`` reference or a remote texture/schema URL) cannot
    make the container reach the internal network, S3, or the internet.
    """

    def __init__(self, *args, network_mode: str | None = "none", **kwargs):
        super().__init__(*args, network_mode=network_mode, **kwargs)

    def execute(self, context):
        run_id = context["run_id"]
        job_dir = get_job_dir(run_id)
        self.mounts = [Mount(source=job_dir, target="/work", type="bind")]
        mf.update_step(job_dir, self.task_id, "running")
        try:
            result = super().execute(context)
            mf.update_step(job_dir, self.task_id, "success")
            return result
        except Exception as e:
            mf.update_step(job_dir, self.task_id, "failed", error=str(e))
            raise
