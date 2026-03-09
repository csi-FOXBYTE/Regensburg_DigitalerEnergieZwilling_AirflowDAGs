from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator


def makeConvertCityJSONToCityGMLTask() -> BaseOperator:
  return PythonOperator(
    task_id="convert_cityjson_to_citygml",
    python_callable=lambda **context: print("TODO: convert_cityjson_to_citygml not implemented"),
  )