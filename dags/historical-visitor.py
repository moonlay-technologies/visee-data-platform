from __future__ import annotations
import time
import os
from datetime import timedelta, datetime
import logging
import pendulum
import pandas as pd
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.sql import SQLCheckOperator
from airflow.models import Variable
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import WaiterError
from botocore.exceptions import NoCredentialsError
# from dotenv import load_dotenv

log = logging.getLogger(__name__)

# -------------------Variables------------------------
local_tz = pendulum.timezone("Asia/Jakarta")
date_today = datetime.now()
env =  Variable.get("visee_credential", deserialize_json=True)
aws_key_id = env["aws_key_id"]
aws_secret_key = env["aws_secret_key"]
aws_region_name = env["aws_region_name"]
postgres_visee = env["postgres_visee"]

conf = Variable.get("visee_config", deserialize_json=True)
schedule_interval = conf["schedule_daily"]
database_url=postgres_visee

# -------------------Args------------------------
args = {
    'owner': 'Moonlay',
    'start_date': datetime(2024, 9, 18, tzinfo=local_tz),
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
    # 'depends_on_past': False,
}

# -------------------DAG------------------------
dag = DAG(
    dag_id='visee-historical-visitor',
    default_args=args,
    schedule_interval=schedule_interval, #schedule_interval,
    catchup=False,
    tags=['visee'],
    concurrency=2,
    max_active_runs=3
)

dag.doc_md = """
Visee ETL for Historical Visitor
"""

# -------------------Filter Time-------------------
def getTimeFilter(ti, **kwargs):
    get_execute_times = datetime.now(local_tz) - timedelta(days=1)
    today = get_execute_times.strftime("%Y-%m-%d")

    ti.xcom_push(key='filter_date', value=today)

get_time_filter = PythonOperator(
    task_id='get_time_filter',
    python_callable=getTimeFilter,
    provide_context=True,
    dag=dag
)

# ------------------ETL-------------------
live_to_historical_visitor = PostgresOperator(
    task_id='to_historical_visitor',
    postgres_conn_id='postgres_visee',
    sql='sql/historical-visitor.sql',
    dag=dag
)

# ---------------------------DAG Flow----------------------------
get_time_filter >> live_to_historical_visitor