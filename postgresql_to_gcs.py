import os

from airflow import models
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "data-bootcamp-terraforms")
GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET_NAME", "INVALID BUCKET NAME")
FILENAME = "test_file"
SQL_QUERY = "select * from netflix_table;"

with models.DAG(
    dag_id='example_postgres_to_gcs',
    schedule_interval='@once',  # Override to match your needs
    start_date=days_ago(1),
    tags=['example'],
) as dag:
    upload_data = PostgresToGCSOperator(
        task_id="get_data", sql=SQL_QUERY, bucket=GCS_BUCKET, filename=FILENAME, gzip=False
    )

    upload_data_server_side_cursor = PostgresToGCSOperator(
        task_id="get_data_with_server_side_cursor",
        sql=SQL_QUERY,
        bucket=GCS_BUCKET,
        filename=FILENAME,
        gzip=False,
        use_server_side_cursor=True,
    )