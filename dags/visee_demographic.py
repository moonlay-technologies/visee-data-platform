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
table_name = 'demographic_raw'
# database_url=postgres_visee
database_url=postgres_local
# -------------------Args------------------------
args = {
    'owner': 'Moonlay',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 30, tzinfo=local_tz),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}
# -------------------DAG------------------------
dag = DAG(
    dag_id='visee_demographic_etl',
    default_args=args,
    concurrency=2,
    catchup=False,
    tags=['visee']
)
# -------------------Initial Task------------------------
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

# -------------------Filtering------------------------
def get_filters(ti, **kwargs):
    get_execute_times = datetime.now(local_tz)
    get_offset_time =get_execute_times.strftime("%Y-%m-%d %H:%M:%S.%f%z")

    get_offset = get_execute_times.strftime("%z")

    formated_times = datetime.strptime(get_offset_time, "%Y-%m-%d %H:%M:%S.%f%z")

    filter_start = (formated_times - timedelta(minutes=5)).replace(second=1, microsecond=1)
    filter_end = formated_times.replace(second=0, microsecond=0)

    log.info(f"filter_start: {filter_start}")
    log.info(f"filter_end: {filter_end}")

    ti.xcom_push(key='filter_start', value=filter_start.strftime("%Y-%m-%d %H:%M:%S.%f") + filter_start.strftime("%z")[:3] + ':' + filter_start.strftime("%z")[3:])
    ti.xcom_push(key='filter_end', value=filter_end.strftime("%Y-%m-%d %H:%M:%S.%f") + filter_end.strftime("%z")[:3] + ':' + filter_end.strftime("%z")[3:])

def test_filter (ti, **kwargs):
    filter_start = '2024-01-22T00:00:00' 
    filter_end = '2024-01-31T23:59:59' 

    filter_start_datetime = datetime.strptime(filter_start, "%Y-%m-%dT%H:%M:%S")
    filter_end_datetime = datetime.strptime(filter_end, "%Y-%m-%dT%H:%M:%S")

    # Push formatted strings with timezone offset to XCom
    ti.xcom_push(key='filter_start', value=filter_start_datetime.strftime("%Y-%m-%d %H:%M:%S.%f") + '+07:00')
    ti.xcom_push(key='filter_end', value=filter_end_datetime.strftime("%Y-%m-%d %H:%M:%S.%f") + '+07:00')


# def filter_date(ti, **kwargs):
#     date_today = datetime.now()
#     date_today = date_today.strftime("%Y-%m-%d")
    
#     log.info((f"filter_date: {date_today}"))

#     ti.xcom_push(key='date_today', value=date_today)

get_filter = PythonOperator(
    task_id = 'get_filter',
    python_callable = test_filter,
    provide_context=True,
    dag=dag
)

# -------------------Task------------------------
def dynamodb_to_postgres(filter_start, filter_end, **kwargs):
    dynamodb = boto3.resource('dynamodb',
                             aws_access_key_id=aws_key_id,
                             aws_secret_access_key=aws_secret_key,
                             region_name=aws_region_name
                             )
    table = dynamodb.Table('demographic_raw')
    ##filter_start_datetime = filter_date
    filter_start_datetime = filter_start
    filter_end_datetime = filter_end

    log.info(f"Filtering data from DynamoDB table {filter_start_datetime}")

    #filter_expression = (Attr('created_at').begins_with(filter_start_datetime))
    filter_expression = (Attr('created_at').gte(filter_start_datetime))

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

to_demographic_raw = PythonOperator(
    task_id='dynamo_to_postgres',
    python_callable=dynamodb_to_postgres,
    provide_context=True,
    op_kwargs={
#        'filter_date': '{{ ti.xcom_pull(task_ids="task_get_filter", key="date_today") }}'
        'filter_start': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_start") }}',
        'filter_end': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_end") }}'
    },
    dag=dag
)

raw_to_demographic = SQLExecuteQueryOperator(
    task_id='to_demographic',
    conn_id='postgres_local',
    sql='sql/to_demographic.sql',
    parameters={
        'filter_start': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_start") }}',
        'filter_end': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_end") }}'
    },
    dag=dag
)

demographic_to_history = SQLExecuteQueryOperator(
    task_id='to_demographic_history',
    conn_id='postgres_local',
    sql='sql/to_demographic_history.sql',
    parameters={
        'filter_start': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_start") }}',
        'filter_end': '{{ ti.xcom_pull(task_ids="get_filter", key="filter_end") }}'
    },
    dag=dag
)

# ---------------------------DAG Flow----------------------------
start_task >> delay_task >> get_filter >> to_demographic_raw >> raw_to_demographic >> end_task #demographic_to_history >> end_task