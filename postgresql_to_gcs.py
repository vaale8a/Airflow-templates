import os
from airflow import DAG
from tempfile import NamedTemporaryFile
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from datetime import timedelta
from datetime import datetime


default_args = {
    'owner': 'grisell.reyes',
    'depends_on_past': False,    
    'start_date': datetime(2021, 10, 1),
    'email': ['grisell.reyes@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

#name the DAG and configuration
dag = DAG('postgres_to_gcs',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)


# Change these to your identifiers, if needed.
GOOGLE_CONN_ID = "google_cloud_default"
POSTGRES_CONN_ID = "postgres_default"
FILENAME = "cities.parquet"
SQL_QUERY = "select * from cities"
bucket_name = "data-bootcamp-terraforms-us"

upload_data = PostgresToGCSOperator(
        task_id="get_data", sql=SQL_QUERY, bucket=bucket_name, filename=FILENAME, gzip=False)
        
upload_data_server_side_cursor = PostgresToGCSOperator(
        task_id="get_data_with_server_side_cursor",
        sql=SQL_QUERY,
        bucket=bucket_name,
        filename=FILENAME,
        gzip=False,
        use_server_side_cursor=True)

upload_data >> upload_data_server_side_cursor





""" def copy_to_gcs(copy_sql, file_name, bucket_name):
    gcs_hook = GoogleCloudStorageHook(GOOGLE_CONN_ID)
    pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)

    with NamedTemporaryFile(suffix=".parquet") as temp_file:
        temp_name = temp_file.name        
        pg_hook.copy_expert(SQL_QUERY, filename=temp_name)
        gcs_hook.upload(bucket_name, file_name, temp_name)

task1 = get_all_pets = PostgresOperator(
    task_id="get_all_cities",
    postgres_conn_id="postgres_default",
    sql="SELECT * FROM cities")

task2 = PythonOperator(task_id='csv_to_gcs',
                   provide_context=True,
                   python_callable=copy_to_gcs,
                   op_kwargs={"copy_sql": SQL_QUERY,
                    "file_name": FILENAME,
                    "bucket_name": bucket_name},
                    dag = dag)

 """

