import os

from airflow import models
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from datetime import timedelta
from datetime import datetime


PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "data-bootcamp-terraforms")
GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET_NAME", "data-bootcamp-csv-postgresql")
FILENAME = "netflix.csv"
SQL_QUERY = "select * from netflix_table;"

default_args = {
    'owner': 'grisell.reyes',
    'depends_on_past': False,    
    'start_date': datetime(2021, 10, 1),
    'email': ['grisell.reyes@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}


with models.DAG(
    dag_id='example_postgres_to_gcs',
    schedule_interval='@once',  # Override to match your needs
    ) as dag:
    upload_data = PostgresToGCSOperator(
        dag = dag,
        postgres_conn_id= 'postgres_default', 
        task_id="get_data", sql=SQL_QUERY, bucket=GCS_BUCKET, filename=FILENAME, gzip=False
    )

    upload_data_server_side_cursor = PostgresToGCSOperator(
        task_id="get_data_with_server_side_cursor",
        dag = dag,
        postgres_conn_id = 'postgres_default',
        sql=SQL_QUERY,
        bucket=GCS_BUCKET,
        filename=FILENAME,
        gzip=False,
        use_server_side_cursor=True,
    )

upload_data >> upload_data_server_side_cursor