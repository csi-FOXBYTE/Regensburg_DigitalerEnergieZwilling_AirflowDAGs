#!/bin/bash
set -e

AIRFLOW_VERSION="3.1.7"
PYTHON_VERSION="3.12"
VENV_DIR=".airflow-env"

python3 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"
pip install --upgrade pip

pip install "apache-airflow[amazon,docker]==$AIRFLOW_VERSION" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

[ -f "requirements.txt" ] && pip install -r requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

echo "Setup complete. Run: airflow standalone"