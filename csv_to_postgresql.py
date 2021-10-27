import airflow
import os
import pandas as pd
import psycopg2
#import urllib.request
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta
from datetime import datetime
from io import StringIO

#default arguments 

default_args = {
    'owner': 'grisell.reyes',
    'depends_on_past': False,    
    'start_date': datetime(2021, 10, 1),
    'email': ['grisell.reyes@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

#name the DAG and configuration
dag = DAG('insert_data_postgres',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

def file_path(relative_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    split_path = relative_path.split("/")
    new_path = os.path.join(dir, *split_path)
    return new_path

def csv_to_postgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    curr = get_postgres_conn.cursor("cursor")
    # CSV loading to table
    #url = "https://github.com/grisreyesrios/Airflow-templates/blob/main/username.csv"
    #file = urllib.request.urlopen(url)
    with open(file_path("username.csv"), "r") as f:
        next(f)
        df = pd.read_csv(f)
        buffer = StringIO()
        df.to_csv(buffer,header=False)
        buffer.seek(0)
        curr.copy_from(buffer, 'username', sep=",")
        get_postgres_conn.commit()

    #os.getcwd()

#Task 
task1 = PostgresOperator(task_id = 'create_table',
                        sql="""
                        CREATE TABLE IF NOT EXISTS username (    
                            username VARCHAR(255),
                            identifier INTEGER,
                            first_name VARCHAR(255),
                            last_name VARCHAR(255));
                            """,
                            postgres_conn_id= 'postgres_default', 
                            autocommit=True,
                            dag= dag)

task2 = PythonOperator(task_id='csv_to_database',
                   provide_context=True,
                   python_callable=csv_to_postgres,
                   dag=dag)


task1 >> task2