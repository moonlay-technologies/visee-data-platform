from __future__ import annotations

from datetime import timedelta, datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from sqlalchemy import create_engine
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.sql import SQLCheckOperator
from airflow.operators.bash import BashOperator
from dotenv import load_dotenv
import os
from airflow.models import Variable
import pendulum
from boto3.dynamodb.conditions import Attr
import logging


log = logging.getLogger(__name__)
# -------------------Variable------------------------
local_tz = pendulum.timezone("Asia/Jakarta")
date_today = datetime.now()
env =  Variable.get("visee_credential", deserialize_json=True)
aws_key_id = env["aws_key_id"]
aws_secret_key = env["aws_secret_key"]
aws_region_name = env["aws_region_name"]
postgres_local = env["postgres_local_url"]
postgres_visee = env["postgres_visee"]
# job_args = Variable.get("", deserialize_json=True)
table_name = 'visitor_raw'
# database_url=postgres_visee
database_url=postgres_local
# -------------------Args------------------------
args = {
    'owner': 'Moonlay',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 22, tzinfo=local_tz),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}
# -------------------DAG------------------------
dag = DAG(
    dag_id='dag_visee_daily_etl',
    default_args=args,
    concurrency=2,
    catchup=False,
    tags=['visee']
)
# -------------------Task------------------------
start_job = DummyOperator(
    task_id='start_task',
    dag=dag
)

end_job = DummyOperator(
    task_id='end_task',
    dag=dag
)

def filter_date(ti, **kwargs):
    date_today = datetime.now()
    date_today = date_today.strftime("%Y-%m-%d")
    
    ti.xcom_push(key='date_today', value=date_today)

def test_date(ti, **kwargs):
    date_today = '2024-01-25'
    ti.xcom_push(key='date_today', value=date_today)

get_filter = PythonOperator(
    task_id = 'task_get_filter',
    # python_callable = filter_date,
    python_callable = test_date,
    provide_context=True,
    dag=dag
)

to_history_peak = SQLExecuteQueryOperator(
    task_id='to_history_peak_day',
    conn_id='visee_postgres',
    sql='sql/to_history_peak_day.sql',
    parameters={
        'filter_date': '{{ ti.xcom_pull(task_ids="task_get_filter", key="date_today") }}'
    },
    dag=dag
)

to_history_state = SQLExecuteQueryOperator(
    task_id='to_history_state',
    conn_id='visee_postgres',
    sql='sql/to_history_state.sql',
    parameters={
        'filter_date': '{{ ti.xcom_pull(task_ids="task_get_filter", key="date_today") }}'
    },
    dag=dag
)

# ------- Create Flow -------
start_job >> get_filter >> to_history_peak >> to_history_state >> end_job