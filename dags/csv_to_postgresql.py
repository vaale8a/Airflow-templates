import airflow
import os
import airflow.utils.dates
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta
from datetime import datetime
from psycopg2.extras import execute_values

#default arguments 

default_args = {
    'owner': 'grisell.reyes',
    'depends_on_past': False,    
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['grisell.reyes@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}


dag = DAG('insert_data_postgres', default_args = default_args, schedule_interval='@once')

dag = DAG('example_python',
          default_args=default_args,
          schedule_interval='@once',
          start_date=datetime(2017, 3, 20), 
          catchup=False)

def csvToPostgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    curr = get_postgres_conn.cursor("cursor")
    # CSV loading to table.
    with open('/Users/grisell.reyes/data-bootcamp-terraforms/kubernetes/netflix_titles.csv', 'r') as f:
        next(f)
        curr.copy_from(f, 'netflix_table', sep=',')
        get_postgres_conn.commit()


task1 = PostgresOperator(task_id = 'create_table',
                        sql="""
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
                            description VARCHAR NOT NULL);
                            """,
                            postgres_conn_id='postgres_default', 
                            autocommit=True,
                            dag= dag)

task2 = PythonOperator(task_id='csv_to_db',
                   provide_context=False,
                   python_callable=csvToPostgres,
                   dag=dag)


task1 >> task2