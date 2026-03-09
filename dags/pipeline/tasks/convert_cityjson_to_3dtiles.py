from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator


def makeConvertCityJSONTo3DTilesTask() -> BaseOperator:
  return PythonOperator(
    task_id="convert_cityjson_to_3dtiles",
    python_callable=lambda **context: print("TODO: convert_cityjson_to_3dtiles not implemented"),
  )