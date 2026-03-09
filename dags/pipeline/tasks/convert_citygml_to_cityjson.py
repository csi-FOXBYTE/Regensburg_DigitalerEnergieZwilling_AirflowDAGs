from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator


def makeConvertCityGMLToCityJSONTask() -> BaseOperator:
  return PythonOperator(
    task_id="convert_citygml_to_cityjson",
    python_callable=lambda **context: print("TODO: convert_citygml_to_cityjson not implemented"),
  )