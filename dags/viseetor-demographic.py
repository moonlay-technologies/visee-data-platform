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
schedule_interval = conf["schedule_interval"]
database_url=postgres_visee
table_name = 'viseetor_raw'

# -------------------Args------------------------
args = {
    'owner': 'Moonlay',
    'start_date': datetime(2024, 9, 19, tzinfo=local_tz),
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
    # 'depends_on_past': False,
}

# -------------------DAG------------------------
dag = DAG(
    dag_id='viseetor-demographic',
    default_args=args,
    schedule_interval=schedule_interval,
    catchup=False,
    tags=['visee'],
    concurrency=2,
    max_active_runs=3
)

dag.doc_md = """
Visee ETL for Demographic
"""

# -------------------Filter Time-------------------
def getTimeFilter(ti, **kwargs):
    get_execute_times = datetime.now(local_tz)
    get_offset_time =get_execute_times.strftime("%Y-%m-%d %H:%M:%S.%f%z")
    get_today = get_execute_times.strftime("%Y-%m-%d")

    get_offset = get_execute_times.strftime("%z")

    formated_times = datetime.strptime(get_offset_time, "%Y-%m-%d %H:%M:%S.%f%z")

    filter_start = (formated_times - timedelta(minutes=5)).replace(second=0, microsecond=1)
    filter_end = formated_times.replace(second=0, microsecond=0)

    ti.xcom_push(key='filter_start', value=filter_start.strftime("%Y-%m-%d %H:%M:%S%f%z")[:-2] + ':' + filter_start.strftime("%Y-%m-%d %H:%M:%S%f%z")[-2:])
    ti.xcom_push(key='filter_end', value=filter_end.strftime("%Y-%m-%d %H:%M:%S%f%z")[:-2] + ':' + filter_end.strftime("%Y-%m-%d %H:%M:%S%f%z")[-2:])
    ti.xcom_push(key='filter_date', value=get_today)

# def getTimeFilterTest(ti, **kwargs):
#     start = '2024-09-26T05:30:00' ###
#     end = '2024-09-26T23:59:59'###
#     date = '2024-09-26' ###

#     filter_start = datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")
#     filter_end = datetime.strptime(end, "%Y-%m-%dT%H:%M:%S")

#     ti.xcom_push(key='filter_start', value=filter_start.strftime("%Y-%m-%d %H:%M:%S%f%z")[:-2] + ':' + filter_start.strftime("%Y-%m-%d %H:%M:%S%f%z")[-2:])
#     ti.xcom_push(key='filter_end', value=filter_end.strftime("%Y-%m-%d %H:%M:%S%f%z")[:-2] + ':' + filter_end.strftime("%Y-%m-%d %H:%M:%S%f%z")[-2:])
#     ti.xcom_push(key='filter_date', value=date)

get_time_filter = PythonOperator(
    task_id='get_time_filter',
    python_callable=getTimeFilter,
    provide_context=True,
    dag=dag
)

# ------------------Get Data from Dynamo DB-------------------
def getDataDynamoDB(filter_start, filter_end, **kwargs):
    dynamodb = boto3.resource('dynamodb',
                             aws_access_key_id=aws_key_id,
                             aws_secret_access_key= aws_secret_key,
                             region_name= aws_region_name
                             )
    table = dynamodb.Table('viseetor_raw')
    filter_start_datetime = filter_start
    filter_end_datetime = filter_end
    log.info(f"Filtering data from DynamoDB table between {filter_start_datetime} and {filter_end_datetime}")

    filter_expression = (Attr('created_at').gte(filter_start_datetime)
                        & Attr('created_at').lte(filter_end_datetime))
                        # & Attr('camera_type').eq('far'))
    
    response = table.scan(
        FilterExpression=filter_expression
    )
    items = response.get('Items', [])
    if items:
        log.info(f"Retrieved {len(items)} items from DynamoDB")
        df_raw = pd.DataFrame(items)

        log.info(f"Data types before convert: {df_raw.dtypes}")

        # Convert Data Type & Rename Columns
        df_raw['id'] = df_raw['id'].astype(str)
        df_raw['age'] = df_raw['age'].astype(str)
        df_raw['attributes'] = df_raw['attributes'].astype(str)
        df_raw['camera_type'] = df_raw['camera_type'].astype(str)
        df_raw['client_id'] = df_raw['client_id'].astype('int')
        df_raw['confidence'] = df_raw['confidence'].astype('float')
        df_raw['created_at'] = pd.to_datetime(df_raw['created_at'], utc=True, errors='coerce')
        df_raw['device_id'] = df_raw['device_id'].astype('int')
        df_raw['emotion'] = df_raw['emotion'].astype(str)
        df_raw['gender'] = df_raw['gender'].astype(str)
        df_raw['object_id'] = df_raw['object_id'].astype('int')
        df_raw['recording_time'] = pd.to_datetime(df_raw['recording_time'], utc=True, errors='coerce')
        df_raw['session_id'] = df_raw['session_id'].astype(str)
        df_raw['updated_at'] = pd.to_datetime(df_raw['updated_at'], utc=True, errors='coerce')
        df_raw['zone_id'] = df_raw['zone_id'].astype('int')
      
        log.info(f"Data types after convert: {df_raw.dtypes}")

        # Insert data into PostgreSQL table
        engine = create_engine(database_url)
        df_raw.to_sql(table_name, engine, if_exists='append', index=False)
        log.info("Data written to PostgreSQL successfully.")
    else:
        log.warning("No items found in the DynamoDB table.")

get_data_dynamodb = PythonOperator(
    task_id='get_data_dynamodb',
    python_callable=getDataDynamoDB,
    provide_context=True,
    op_kwargs={
        'filter_start': '{{ ti.xcom_pull(task_ids="get_time_filter", key="filter_start") }}',
        'filter_end': '{{ ti.xcom_pull(task_ids="get_time_filter", key="filter_end") }}'
    },
    dag=dag
)

# ------------------Transform and Load Data-------------------
raw_to_live_demographic = PostgresOperator(
    task_id='to_live_demographic',
    postgres_conn_id='postgres_visee',
    sql='sql/live-demographic.sql', 
    params={
        'filter_date': '{{ ti.xcom_pull(task_ids="get_time_filter", key="filter_date") }}'
    },
    dag=dag
)

# ------------------Truncate Raw Data-------------------
viseetor_raw_truncate = PostgresOperator(
    task_id='truncate_viseetor_raw',
    postgres_conn_id='postgres_visee',
    sql='sql/truncate_viseetor_raw.sql',
    dag=dag
)

# ---------------------------DAG Flow----------------------------
get_time_filter >> get_data_dynamodb >> raw_to_live_demographic >> viseetor_raw_truncate