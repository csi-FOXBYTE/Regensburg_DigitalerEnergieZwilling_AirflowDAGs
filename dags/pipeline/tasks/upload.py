import os

from airflow.providers.standard.operators.python import PythonOperator

from pipeline import manifest as mf
from pipeline import publication
from pipeline.config import (
    PUBLISH_WORKERS,
    get_job_dir,
    sanitize_job_id,
    validate_bucket,
    validate_distinct_buckets,
)
from pipeline.s3_connection import get_s3_hook


def _publish_outputs_callable(
    params,
    run_id,
    task_id,
    outputs,
    aws_conn_id,
    exclusive_bucket_params=(),
):
    """Publish all outputs of a run together via staged, verified replacement.

    ``outputs`` is a list of dicts with ``bucket_param``, ``directories`` and
    optional ``required_keys``/``entry_keys``. They are promoted in list order
    and only pruned once every output has been promoted.
    """
    job_dir = get_job_dir(run_id)
    mf.update_step(job_dir, task_id, "running")
    try:
        roles = {}
        for name in (*exclusive_bucket_params, *(o["bucket_param"] for o in outputs)):
            bucket = params.get(name)
            if not bucket:
                raise ValueError(f"Missing param: {name}")
            roles[name] = validate_bucket(bucket)
        validate_distinct_buckets(roles)

        job_id = sanitize_job_id(run_id)
        plans = [
            publication.build_plan(
                params[output["bucket_param"]],
                job_id,
                [os.path.join(job_dir, d) for d in output["directories"]],
                required_keys=output.get("required_keys", ()),
                entry_keys=output.get("entry_keys", ()),
                label=output["bucket_param"],
            )
            for output in outputs
        ]
        client = get_s3_hook(aws_conn_id).get_conn()
        publication.publish(client, plans, run_id=run_id, workers=PUBLISH_WORKERS)
        mf.update_step(job_dir, task_id, "success")
    except Exception as e:
        mf.update_step(job_dir, task_id, "failed", error=str(e))
        raise


def make_publish_outputs_task(
    task_id: str,
    outputs: list[dict],
    aws_conn_id: str,
    exclusive_bucket_params: tuple[str, ...] = (),
) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=_publish_outputs_callable,
        op_kwargs={
            "task_id": task_id,
            "outputs": outputs,
            "aws_conn_id": aws_conn_id,
            "exclusive_bucket_params": exclusive_bucket_params,
        },
    )
