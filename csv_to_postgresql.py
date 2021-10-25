from datetime import timedelta
from datetime import datetime

import airflow
import os
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

#default arguments to the past 

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,    
    'start_date': datetime(2022, 10, 25),
    #'end_date': datetime(2018, 12, 5),
    'email': ['grisell.reyes@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

schedule_interval = "0 * * * *"

dag = DAG(
    'Airflow-templates', 
    default_args=default_args, 
    schedule_interval=schedule_interval
    )

CSV_FILE_DIR = os.getenv("CSV_FILE_DIR", "/Users/grisell.reyes/Airflow-templates/dags/netflix_titles.csv")
PSQL_DB = os.getenv("PSQL_DB", "airflow")
PSQL_USER = os.getenv("PSQL_USER", "airflow")
PSQL_PASSWORD = os.getenv("PSQL_PASSWORD", "airflow")
PSQL_PORT = os.getenv("PSQL_PORT", "5432")
PSQL_HOST = os.getenv("PSQL_HOST", "localhost")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "data-bootcamp-terraforms")



def csvToPostgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='airflow_db')
    get_postgres_conn = PostgresHook(postgres_conn_id='airflow_db').get_conn()
    curr = get_postgres_conn.cursor("cursor")
    # CSV loading to table.
    with open('/Users/grisell.reyes/Airflow-templates/dags/netflix_titles.csv', 'r') as f:
        next(f)
        curr.copy_from(f, 'netflix_table', sep=',')
        get_postgres_conn.commit()


task1 = PostgresOperator(task_id = 'create_table',
                         sql = ("""
            CREATE TABLE IF NOT EXISTS netflix (
            show_id SERIAL PRIMARY KEY,
            type VARCHAR NOT NULL,
            title VARCHAR NOT NULL,
            director VARCHAR NOT NULL,
            cast VARCHAR NOT NULL,
            country VARCHAR NOT NULL,
            date_added DATE NOT NULL,
            release_year INTEGER NOT NULL,
            rating VARCHAR NOT NULL,
            duration VARCHAR NOT NULL,
            listed_in VARCHAR NOT NULL,
            description VARCHAR NOT NULL               
            );
          """),
                         postgres_conn_id='airflow_db', 
                         autocommit=True,
                         dag= dag)

task2 = PythonOperator(task_id='csv_to_db',
                   provide_context=False,
                   python_callable=csvToPostgres,
                   dag=dag)


task1 >> task2 





