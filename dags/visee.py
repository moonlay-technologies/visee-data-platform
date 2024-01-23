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
table_name = 'visitor_raw'
# database_url=postgres_visee
database_url=postgres_local
# -------------------Args------------------------
args = {
    'owner': 'Moonlay',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 15, tzinfo=local_tz),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}
# -------------------DAG------------------------
dag = DAG(
    dag_id='dag_visee_etl',
    default_args=args,
    concurrency=2,
    catchup=False,
    tags=['visee']
)
# -------------------Task------------------------
start_task = DummyOperator(
    task_id='start_task', 
    dag=dag)

end_task = DummyOperator(
    task_id='end_task', 
    dag=dag)

delay_task = BashOperator(
    task_id='waiting', 
    bash_command='sleep 3', 
    dag=dag) 

# -----------------------||------------------------
def get_filters(ti, **kwargs):
    get_execute_times = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
    get_time = datetime.strptime(get_execute_times, "%Y-%m-%d %H:%M:%S")

    filter_start = (get_time - timedelta(minutes=5)).replace(second=1, microsecond=1)
    filter_end = (get_time).replace(second=0, microsecond=0)

    log.info(f"filter_start: {filter_start.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"filter_end: {filter_end.strftime('%Y-%m-%d %H:%M:%S')}")

    ti.xcom_push(key='filter_start', value=filter_start.strftime('%Y-%m-%dT%H:%M:%SZ'))
    ti.xcom_push(key='filter_end', value=filter_end.strftime('%Y-%m-%dT%H:%M:%SZ'))

def test_filter (ti, **kwargs):
    filter_start = '2024-01-22T14:55:01Z'
    filter_end = '2024-01-22T15:00:00Z'

    log.info(f"filter_start: {filter_start}")
    log.info(f"filter_end: {filter_end}")

    ti.xcom_push(key='filter_start', value=filter_start)
    ti.xcom_push(key='filter_end', value=filter_end)

get_filter = PythonOperator(
    task_id='get_filter',
    # python_callable=get_filters,
    python_callable=test_filter,
    provide_context=True,
    dag=dag
)
# -----------------------||------------------------
def dynamodb_to_postgres(filter_start, filter_end, **kwargs):
    dynamodb = boto3.resource('dynamodb',
                             aws_access_key_id=aws_key_id,
                             aws_secret_access_key=aws_secret_key,
                             region_name=aws_region_name
                             )
    table = dynamodb.Table('viseetor_raw')
    filter_start_datetime = datetime.strptime(filter_start, "%Y-%m-%dT%H:%M:%SZ")
    filter_end_datetime = datetime.strptime(filter_end, "%Y-%m-%dT%H:%M:%SZ")

    log.info(f"Filtering data from DynamoDB table between {filter_start_datetime} and {filter_end_datetime}")

    filter_expression = (Attr('created_at').gte(filter_start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")) &
                         Attr('updated_at').lte(filter_end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")))

    response = table.scan(
        FilterExpression=filter_expression
    )
    items = response.get('Items', [])
    if items:
        log.info(f"Retrieved {len(items)} items from DynamoDB")

        # Convert DynamoDB items to DataFrame
        df_raw = pd.DataFrame(items)
        log.info(f"Data types before insertion: {df_raw.dtypes}")
        # Insert data into PostgreSQL table
        engine = create_engine(database_url)
        df_raw.to_sql(table_name, engine, if_exists='append', index=False)

        log.info("Data written to PostgreSQL successfully.")
    else:
        log.warning("No items found in the DynamoDB table.")

to_visitor_raw = PythonOperator(
    task_id='dynamo_to_postgres',
    python_callable=dynamodb_to_postgres,
    provide_context=True,
    op_kwargs={
        'filter_start': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_start") }}',
        'filter_end': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_end") }}'
    },
    dag=dag
)
# -----------------------||------------------------
raw_to_visitor = SQLExecuteQueryOperator(
    task_id='to_visitor',
    conn_id='postgre_local',
    sql='sql/to_visitor.sql',
    parameters={
        'filter_start': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_start") }}',
        'filter_end': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_end") }}'
    },
    dag=dag
)

visitor_to_monitor_state = SQLExecuteQueryOperator(
    task_id='to_monitor_state',
    conn_id='postgre_local',
    sql='sql/to_monitor_state.sql',
    dag=dag
)

visitor_to_monitor_peak = SQLExecuteQueryOperator(
    task_id='to_monitor_peak',
    conn_id='postgre_local',
    sql='sql/to_monitor_peak.sql',
    parameters={
        'filter_start': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_start") }}',
        'filter_end': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_end") }}'
    },
    dag=dag
)
# ---------------------------DAG Flow----------------------------
start_task >> delay_task >> get_filter >> to_visitor_raw  >> raw_to_visitor >> [visitor_to_monitor_state, visitor_to_monitor_peak] >> end_task