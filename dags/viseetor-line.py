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

# # from airflow.operators.email import EmailOperator
# # email_subject_success = "Visee ETL Job Succeeded"
# # email_subject_failure = "Visee ETL Job Failed"
# # email_recipients = ["ivan.rivaldo@moonlay.com","astrid.anggraini@moonlay.com"]


log = logging.getLogger(__name__)

# -------------------Variables------------------------
local_tz = pendulum.timezone("Asia/Jakarta")
date_today = datetime.now()
env =  Variable.get("visee_credential", deserialize_json=True)
aws_key_id = env["aws_key_id"]
aws_secret_key = env["aws_secret_key"]
aws_region_name = env["aws_region_name"]
postgres_visee = env["postgres_visee"]
postgres_local = env["postgres_local_url"]

# conf = Variable.get("visee_config", deserialize_json=True)
# schedule_interval = conf["schedule_interval"]
# database_url=postgres_visee
database_url = postgres_local
table_name = 'viseetor_line'

# -------------------Args------------------------
args = {
    'owner': 'Moonlay',
    'start_date': datetime(2024, 9, 18, tzinfo=local_tz),
    'retries': 2,
    'retry_delay': timedelta(seconds=90)
    # 'depends_on_past': False,
}

# -------------------DAG------------------------
dag = DAG(
    dag_id='dag_live_visitor_etl',
    default_args=args,
    schedule_interval='@daily', #schedule_interval,
    catchup=False,
    tags=['visee'],
    concurrency=2,
    max_active_runs=3
)

dag.doc_md = """
Visee ETL for Visitor
"""

# -------------------Dummy Task------------------------
start_task = DummyOperator(
    task_id='start_task', 
    dag=dag)

end_task = DummyOperator(
    task_id='end_task', 
    dag=dag)

# -------------------Filter Time-------------------
def get_filter_time(ti, **kwargs):
    get_execute_times = datetime.now(local_tz)
    get_offset_time =get_execute_times.strftime("%Y-%m-%d %H:%M:%S.%f%z")
    get_today = get_execute_times.strftime("%Y-%m-%d")

    get_offset = get_execute_times.strftime("%z")

    formated_times = datetime.strptime(get_offset_time, "%Y-%m-%d %H:%M:%S.%f%z")

    filter_start = (formated_times - timedelta(minutes=5)).replace(second=0, microsecond=1)
    filter_end = formated_times.replace(second=0, microsecond=0)

    log.info(f"filter_start: {filter_start}")
    log.info(f"filter_end: {filter_end}")
    log.info(f"filter_date: {get_today}")

    ti.xcom_push(key='filter_start', value=filter_start.strftime("%Y-%m-%d %H:%M:%S.%f") + filter_start.strftime("%z")[:3] + ':' + filter_start.strftime("%z")[3:])
    ti.xcom_push(key='filter_end', value=filter_end.strftime("%Y-%m-%d %H:%M:%S.%f") + filter_end.strftime("%z")[:3] + ':' + filter_end.strftime("%z")[3:])
    ti.xcom_push(key='filter_date', value=get_today)

def test_filter_time (ti, **kwargs):
    filter_start = '2024-09-18T06:00:00' ###
    filter_end = '2024-09-18T23:59:59'###
    filter_date = '2024-09-18' ###

    filter_start_datetime = datetime.strptime(filter_start, "%Y-%m-%dT%H:%M:%S")
    filter_end_datetime = datetime.strptime(filter_end, "%Y-%m-%dT%H:%M:%S")

    ti.xcom_push(key='filter_start', value=filter_start_datetime.strftime("%Y-%m-%d %H:%M:%S.%f") + '+07:00')
    ti.xcom_push(key='filter_end', value=filter_end_datetime.strftime("%Y-%m-%d %H:%M:%S.%f") + '+07:00')
    ti.xcom_push(key='filter_date', value=filter_date)

get_time_filter = PythonOperator(
    task_id='get_filter',
    # python_callable=get_filter_time,
    python_callable=test_filter_time,
    provide_context=True,
    dag=dag
)

# ------------------Get Data from Dynamo DB-------------------
def dynamodb_to_postgres(filter_start, filter_end, **kwargs):
    dynamodb = boto3.resource('dynamodb',
                             aws_access_key_id=aws_key_id,
                             aws_secret_access_key= aws_secret_key,
                             region_name= aws_region_name
                             )
    table = dynamodb.Table('viseetor_line')
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

        # Sort DataFrame by 'created_at'
        # df_raw.sort_values(by='created_at', inplace=True, ascending=True)

        # # Get the first row after sorting
        # sorted_data = df_raw.head(10000)
        log.info(f"Data types before convert: {df_raw.dtypes}")

        # Convert Data Type & Rename Columns
        df_raw['created_at'] = pd.to_datetime(df_raw['created_at'], utc=True, errors='coerce')
        df_raw['recording_time'] = pd.to_datetime(df_raw['recording_time'], utc=True, errors='coerce')
        df_raw['client_id'] = df_raw['client_id'].astype('int')
        df_raw['zone_id'] = df_raw['zone_id'].astype('int')
        df_raw['device_id'] = df_raw['device_id'].astype('int')
        df_raw['visitor_peak'] = df_raw['visitor_peak'].astype('int')
        df_raw['female_peak'] = df_raw['female_peak'].astype('int')
        df_raw['male_peak'] = df_raw['male_peak'].astype('int')
        df_raw['visitor_in'] = df_raw['visitor_in'].astype('int')
        df_raw['female_in'] = df_raw['female_in'].astype('int')
        df_raw['male_in'] = df_raw['male_in'].astype('int')
        df_raw['visitor_out'] = df_raw['visitor_out'].astype('int')
        df_raw['female_out'] = df_raw['female_out'].astype('int')
        df_raw['male_out'] = df_raw['male_out'].astype('int')
        df_raw['camera_type'] = df_raw['camera_type'].astype(str)
        df_raw['id_dynamo'] = df_raw['id'].astype(str)

        log.info(f"Data types after convert: {df_raw.dtypes}")

        # Insert data into PostgreSQL table
        engine = create_engine(database_url)
        df_raw.to_sql(table_name, engine, if_exists='append', index=False)
        log.info("Data written to PostgreSQL successfully.")
    else:
        log.warning("No items found in the DynamoDB table.")

get_data_dynamodb = PythonOperator(
    task_id='dynamo_to_postgres',
    python_callable=dynamodb_to_postgres,
    provide_context=True,
    op_kwargs={
        'filter_start': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_start") }}',
        'filter_end': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_end") }}'
    },
    dag=dag
)

# ------------------Transform and Load Data-------------------
raw_line_to_live_visitor = PostgresOperator(
    task_id='to_live_visitor',
    postgres_conn_id='postgres_local',
    sql='sql/live-visitor.sql',
    params={
        'filter_date': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_date") }}'
    },
    dag=dag
)

# ---------------------------DAG Flow----------------------------
start_task >> get_time_filter >> get_data_dynamodb >> raw_line_to_live_visitor >> end_task