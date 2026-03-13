from airflow.providers.standard.operators.python import PythonOperator
import shutil
import os
from pipeline.config import WORK_DIR
from airflow.sdk import TriggerRule


def _cleanup_callable(directories: list[str]):
    for directory in directories:
        dir_path = os.path.join(WORK_DIR, directory)
        if os.path.exists(dir_path):
           shutil.rmtree(dir_path)
        else:
            print(f"Directory does not exist: {dir_path}")    

def make_cleanup_task(directories: list[str]) -> PythonOperator:
    return PythonOperator(
        task_id="cleanup",
        python_callable=_cleanup_callable,
        op_kwargs={"directories": directories},
        trigger_rule= TriggerRule.ALL_DONE
    )