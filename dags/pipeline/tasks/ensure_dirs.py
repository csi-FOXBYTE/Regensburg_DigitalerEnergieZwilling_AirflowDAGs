from airflow.providers.standard.operators.python import PythonOperator
import os
from pipeline.config import WORK_DIR


def ensure_dirs(subdirs: list[str]):
    for subdir in subdirs:
        path = os.path.join(WORK_DIR, subdir)
        os.makedirs(path, exist_ok=True)
        print(path)

def make_ensure_dirs_task(subdirs: list[str]) -> PythonOperator:
    return PythonOperator(
        task_id="ensure_dirs_task",
        python_callable=ensure_dirs,
        op_kwargs={"subdirs": subdirs},
    )