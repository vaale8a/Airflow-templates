from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow import models


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,    
    'start_date': datetime(2022, 10, 22),
    #'end_date': datetime(2018, 12, 5),
    'email': ['grisell.reyes@wizeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

schedule_interval = "0 * * * *"

dag = DAG(
    'Airflow-templates', 
    default_args=default_args, 
    schedule_interval=schedule_interval
    )


db_create_body = {
    "instance": 'data-bootcamp-1',
    "name": 'data-bootcamp-netflix',
    "project": 'data-bootcamp-terraforms'
}

with models.DAG(JOB_NAME,
                schedule_interval="@hourly",
                default_args=default_args) as dag:

    sql_db_create_task = CloudSqlInstanceDatabaseCreateOperator(
    project_id='data-bootcamp-terraforms',
    body=db_create_body,
    instance='data-bootcamp-1',
    task_id='sql_db_create_task'
)
    sql_db_create_task
