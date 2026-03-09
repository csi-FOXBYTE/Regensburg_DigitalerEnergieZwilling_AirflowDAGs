from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator


def makeEnrichCityJSONTask() -> BaseOperator:
  return PythonOperator(
    task_id="enrich_cityjson",
    python_callable=lambda **context: print("TODO: enrich_cityjson not implemented"),
  )