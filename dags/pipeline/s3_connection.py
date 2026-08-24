from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import BaseHook


def get_s3_hook(aws_conn_id: str) -> S3Hook:
    """Return an S3 hook only when the explicitly named connection exists."""
    connection = BaseHook.get_connection(aws_conn_id)
    if connection.conn_type != "aws":
        raise ValueError(
            f"Airflow connection {aws_conn_id!r} must have connection type 'aws'"
        )
    return S3Hook(aws_conn_id=aws_conn_id)
