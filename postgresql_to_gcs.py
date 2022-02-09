import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from datetime import timedelta
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

"""
Extract data from Postgres > Load into GCS 
"""


default_args = {
    'owner': 'valeria.ochoa',
    'depends_on_past': False,    
    'start_date': datetime(2022, 1, 1),
    'email': ['valeria.ochoa@wizeline.com'],
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
GOOGLE_CONN_ID = "google_cloud_defaultt"
POSTGRES_CONN_ID = "postgres_default"
FILENAME = "cities.parquet"
SQL_QUERY = "select * from cities"
bucket_name = "milestone-2-3"


upload_data = PostgresToGCSOperator(
        task_id="get_data", sql=SQL_QUERY, bucket=bucket_name, filename=FILENAME, gzip=False, dag=dag)
        
upload_data_server_side_cursor = PostgresToGCSOperator(
        task_id="get_data_with_server_side_cursor",
        sql=SQL_QUERY,
        bucket=bucket_name,
        filename=FILENAME,
        gzip=False,
        use_server_side_cursor=True,
        export_format='parquet',
        dag=dag)

task = PythonOperator(task_id='print',
                   provide_context=True,
                   python_callable=justprint,
                   dag=dag)



task >> upload_data >> upload_data_server_side_cursor