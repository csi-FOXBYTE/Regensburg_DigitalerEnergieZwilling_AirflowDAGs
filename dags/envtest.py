from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import os

def printEnv():
  print(os.getenv("CITYJSON_WORK_DIR", "No env found"))

with DAG(
  dag_id="env-test",
  schedule=None
) as dag:
  printAWS = PythonOperator(
    task_id="env_Test_Task",
    python_callable=printEnv
  )