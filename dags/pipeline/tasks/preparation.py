import os
from airflow.providers.standard.operators.python import PythonOperator
from pipeline.config import get_job_dir, sanitize_job_id
from pipeline import manifest as mf


def _preparation_callable(subdirs: list[str], dag_id: str, step_names: list[str], params, run_id):
    job_id = sanitize_job_id(run_id)
    job_dir = get_job_dir(run_id)
    for subdir in subdirs:
        os.makedirs(os.path.join(job_dir, subdir), exist_ok=True)
    mf.create_manifest(
        job_dir=job_dir,
        job_id=job_id,
        run_id=run_id,
        dag_id=dag_id,
        params=dict(params),
        step_names=step_names,
    )
    mf.update_step(job_dir, "preparation", "running")
    mf.update_step(job_dir, "preparation", "success")


def make_preparation_task(subdirs: list[str], dag_id: str, step_names: list[str]) -> PythonOperator:
    return PythonOperator(
        task_id="preparation",
        python_callable=_preparation_callable,
        op_kwargs={"subdirs": subdirs, "dag_id": dag_id, "step_names": step_names},
    )
