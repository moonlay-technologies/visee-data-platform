from datetime import timedelta, datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from sqlalchemy import create_engine
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.sql import SQLCheckOperator


args = {
    'owner': 'Bun',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='dag_visee_2',
    default_args=args,
    concurrency=2,
    catchup=False,
    tags=['visee']
)

table_name = 'visitor_raw'
database_url = 'postgresql+psycopg2://postgres:P%40ssw0rd123@host.docker.internal:5432/postgres'

def dynamodb_to_postgres():
    dynamodb = boto3.resource('dynamodb',
                           aws_access_key_id='AKIAT7CSIS7IAVIA7FFJ',
                           aws_secret_access_key='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',
                           region_name='us-east-1'
                        #    endpoint_url='http://172.19.0.2:8000'
                           )
    
    table = dynamodb.Table('raw_data')
    
    response = table.scan()  # Assuming you want to scan the entire table
    items = response.get('Items', [])
    
    if items:
        df_raw = pd.DataFrame(items)
        engine = create_engine(database_url)
        df_raw.to_sql(table_name, engine, if_exists='append', index=False)
    else:
        print("No items found in the DynamoDB table.")


to_visitor_raw = PythonOperator(
    task_id='dynamo_to_postgres',
    python_callable=dynamodb_to_postgres,
    provide_context=True,
    dag=dag
)

raw_to_visitor = PostgresOperator(
    task_id='to_visitor',
    postgres_conn_id='postgre_local',
    sql='sql/to_visitor.sql',
    dag=dag
)

visitor_to_monitor_state = PostgresOperator(
    task_id='to_monitor_state',
    postgres_conn_id='postgre_local',
    sql='sql/to_monitor_state.sql',
    dag=dag
)

visitor_to_monitor_peak = PostgresOperator(
    task_id='to_monitor_peak',
    postgres_conn_id='postgre_local',
    sql='sql/to_monitor_peak.sql',
    dag=dag
)

start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> to_visitor_raw  >> raw_to_visitor >> [visitor_to_monitor_state, visitor_to_monitor_peak] >> end_task
