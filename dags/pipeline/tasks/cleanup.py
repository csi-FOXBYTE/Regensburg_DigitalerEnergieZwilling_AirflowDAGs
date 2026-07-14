from airflow.providers.standard.operators.python import PythonOperator
import shutil
import os
from pipeline.config import get_job_dir
from pipeline import manifest as mf


def _cleanup_callable(directories: list[str], run_id, task_id):
    job_dir = get_job_dir(run_id)
    mf.update_step(job_dir, task_id, "running")
    try:
        for directory in directories:
            dir_path = os.path.join(job_dir, directory)
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
        mf.update_step(job_dir, task_id, "success")
    except Exception as e:
        mf.update_step(job_dir, task_id, "failed", error=str(e))
        raise


def make_cleanup_task(directories: list[str]) -> PythonOperator:
    return PythonOperator(
        task_id="cleanup",
        python_callable=_cleanup_callable,
        op_kwargs={"directories": directories, "task_id": "cleanup"},
    )
