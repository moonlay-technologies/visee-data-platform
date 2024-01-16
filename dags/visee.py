from datetime import timedelta, datetime

import json
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.sql import SQLCheckOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
import pandas as pd


args = {
    'owner': 'Bun',
    'depends_on_past': False,  # Fix the typo in depends_on_past
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='dag_visee',
    default_args=args,
    concurrency=2,
    catchup=False,
    tags=['visee']
)

table_name = 'visitor_raw'
database_url = 'postgresql+psycopg2://postgres:P%40ssw0rd123@host.docker.internal:5432/postgres'
json_file_path = 'dags/jobs/files/visitor_raw.json'

def get_json_and_insert():
    df_raw = pd.read_json(json_file_path)
    engine = create_engine(database_url)
    df_raw.to_sql(table_name, engine, if_exists='append', index=False)

json_to_table = PythonOperator(
    task_id='to_visitor_raw',
    python_callable=get_json_and_insert,
    provide_context=True,
    dag=dag
)

raw_to_visitor = PostgresOperator(
    task_id='to_visitor',
    postgres_conn_id='postgre_local',
    sql='sql/to_visitor.sql',
    # provide_context=True,
    dag=dag
)

start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> json_to_table >> raw_to_visitor >> end_task