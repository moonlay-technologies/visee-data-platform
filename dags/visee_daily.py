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

database_url=postgres_visee
# database_url=postgres_local
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
#--------------------------------------------------------------#
def get_filters(ti, **kwargs):
    get_execute_times = datetime.now(local_tz)
    get_offset_time =get_execute_times.strftime("%Y-%m-%d %H:%M:%S.%f%z")
    get_today = get_execute_times.strftime("%Y-%m-%d")

    get_offset = get_execute_times.strftime("%z")

    formated_times = datetime.strptime(get_offset_time, "%Y-%m-%d %H:%M:%S.%f%z")

    filter_start = formated_times.replace(hour=6,minute=0,second=0, microsecond=0)
    filter_end = formated_times.replace(hour=23,minute=59, second=59, microsecond=999999)

    log.info(f"filter_start: {filter_start}")
    log.info(f"filter_end: {filter_end}")
    log.info(f"filter_date: {get_today}")

    ti.xcom_push(key='filter_start', value=filter_start.strftime("%Y-%m-%d %H:%M:%S.%f") + filter_start.strftime("%z")[:3] + ':' + filter_start.strftime("%z")[3:])
    ti.xcom_push(key='filter_end', value=filter_end.strftime("%Y-%m-%d %H:%M:%S.%f") + filter_end.strftime("%z")[:3] + ':' + filter_end.strftime("%z")[3:])
    ti.xcom_push(key='filter_date', value=get_today)

def test_filter (ti, **kwargs):
    filter_start = '2024-03-13T06:00:00' 
    filter_end = '2024-03-13T23:59:59'
    filter_date = '2024-03-13' 

    filter_start_datetime = datetime.strptime(filter_start, "%Y-%m-%dT%H:%M:%S")
    filter_end_datetime = datetime.strptime(filter_end, "%Y-%m-%dT%H:%M:%S")

    ti.xcom_push(key='filter_start', value=filter_start_datetime.strftime("%Y-%m-%d %H:%M:%S.%f") + '+07:00')
    ti.xcom_push(key='filter_end', value=filter_end_datetime.strftime("%Y-%m-%d %H:%M:%S.%f") + '+07:00')
    ti.xcom_push(key='filter_date', value=filter_date)

get_filter = PythonOperator(
    task_id = 'task_get_filter',
    # python_callable = get_filters,
    python_callable = test_filter,
    provide_context=True,
    dag=dag
)
#--------------------------------------------------------------#
# def viseetor_line(filter_start, filter_end, **kwargs):
#     dynamodb = boto3.resource('dynamodb',
#                              aws_access_key_id=aws_key_id,
#                              aws_secret_access_key=aws_secret_key,
#                              region_name=aws_region_name
#                              )
#     table = dynamodb.Table('viseetor_line')
#     filter_start_datetime = filter_start
#     filter_end_datetime = filter_end

#     log.info(f"Filtering DynamoDB table between {filter_start_datetime} and {filter_end_datetime}")
#     filter_expression = (Attr('created_at').gte(filter_start_datetime)
#                          & Attr('created_at').lte(filter_end_datetime)
#                          & Attr('camera_type').eq('far'))
    
#     response = table.scan(
#         FilterExpression=filter_expression
#     )
#     items = response.get('Items', [])
#     table_name = 'viseetor_line'
#     if items:
#         log.info(f"Retrieved {len(items)} items from DynamoDB")
#         df_raw = pd.DataFrame(items)

#         log.info(f"Data types before insertion: {df_raw.dtypes}")

#         engine = create_engine(database_url)
#         df_raw.to_sql(table_name, engine, if_exists='replace', index=False)
#         log.info("Data written to PostgreSQL successfully.")
#     else:
#         log.warning("No items found in the DynamoDB table.")

# get_viseetor_line = PythonOperator(
#     task_id='get_data_viseetor_line',
#     python_callable=viseetor_line,
#     provide_context=True,
#     op_kwargs={
#         'filter_start': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_start") }}',
#         'filter_end': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_end") }}'
#     },
#     dag=dag
# )
#--------------------------------------------------------------#
def viseetor_dwell_to_postgress(filter_start, filter_end, **kwargs):
    dynamodb = boto3.resource('dynamodb',
                             aws_access_key_id=aws_key_id,
                             aws_secret_access_key=aws_secret_key,
                             region_name=aws_region_name
                             )
    table = dynamodb.Table('viseetor_dwell')
    filter_start_datetime = filter_start
    filter_end_datetime = filter_end

    log.info(f"Filtering DynamoDB table between {filter_start_datetime} and {filter_end_datetime}")
    filter_expression = (Attr('created_at').gte(filter_start_datetime)
                         & Attr('created_at').lte(filter_end_datetime))
    
    response = table.scan(
        FilterExpression=filter_expression
    )
    items = response.get('Items', [])
    table_name = 'viseetor_dwell'
    if items:
        log.info(f"Retrieved {len(items)} items from DynamoDB")
        df_raw = pd.DataFrame(items)

        log.info(f"Data types before insertion: {df_raw.dtypes}")

        engine = create_engine(database_url)
        df_raw.to_sql(table_name, engine, if_exists='replace', index=False)
        log.info("Data written to PostgreSQL successfully.")
    else:
        log.warning("No items found in the DynamoDB table.")

get_viseetor_dwell = PythonOperator(
    task_id='get_data_viseetor_dwell',
    python_callable=viseetor_dwell_to_postgress,
    provide_context=True,
    op_kwargs={
        'filter_start': '{{ ti.xcom_pull(task_ids="task_get_filter", key="filter_start") }}',
        'filter_end': '{{ ti.xcom_pull(task_ids="task_get_filter", key="filter_end") }}'
    },
    dag=dag
)
#--------------------------------------------------------------#
# to_history_peak = SQLExecuteQueryOperator(
#     task_id='to_history_peak_day',
#     conn_id='visee_postgres',
#     sql='sql/to_history_peak_day.sql',
#     parameters={
#         'filter_date': '{{ ti.xcom_pull(task_ids="task_get_filter", key="filter_date") }}'
#     },
#     dag=dag
# )

# to_history_state = SQLExecuteQueryOperator(
#     task_id='to_history_state',
#     conn_id='visee_postgres',
#     sql='sql/to_history_state.sql',
#     parameters={
#         'filter_date': '{{ ti.xcom_pull(task_ids="task_get_filter", key="filter_date") }}'
#     },
#     dag=dag
# )
#--------------------------------------------------------------#
# start_job >> get_filter >> get_viseetor_line >> get_viseetor_dwell >> to_history_peak >> to_history_state >> end_job
start_job >> get_filter >> get_viseetor_dwell >> end_job