from __future__ import annotations

from datetime import timedelta, datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import pendulum
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import logging

log = logging.getLogger(__name__)

param = Variable.get("test_var", deserialize_json=True)
schedule_interval = param["schedule_interval"]
date_params = param["date_params"]
local_tz = pendulum.timezone("Asia/Jakarta")
date_today = datetime.now()

args = {
    'owner': 'Ivan',
    'start_date': datetime(2024, 1, 13, tzinfo=local_tz),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id='dag_test',
    default_args=args,
    description='Test DAG',
    schedule_interval=schedule_interval,
    catchup=False,
    concurrency=3,
    max_active_runs=4,
    tags=['visee', 'test']
)

start = DummyOperator(
    task_id='start_job',
    dag=dag
)

end = DummyOperator(
    task_id='end_job',
    dag=dag
)

bash_task = BashOperator(
    task_id='delay',
    bash_command='sleep 3',
    dag=dag
)

def python_function(ti, **kwargs):
    get_execute_times = datetime.now(local_tz)
    get_formated = get_execute_times.strftime("%Y-%m-%d %H:%M:%S.%f%z")
    
    get_offset = get_execute_times.strftime("%z")

    formated_times = datetime.strptime(get_formated, "%Y-%m-%d %H:%M:%S.%f%z")

    filter_start = (formated_times - timedelta(minutes=5)).replace(second=1, microsecond=1)
    filter_end = formated_times.replace(second=0, microsecond=0)

    ti.xcom_push(key='filter_start', value=filter_start.strftime("%Y-%m-%d %H:%M:%S.%f") + filter_start.strftime("%z")[:3] + ':' + filter_start.strftime("%z")[3:])
    ti.xcom_push(key='filter_end', value=filter_end.strftime("%Y-%m-%d %H:%M:%S.%f") + filter_end.strftime("%z")[:3] + ':' + filter_end.strftime("%z")[3:])

    # filter_start = (get_time - timedelta(minutes=5)).replace(second=1, microsecond=1)
    # filter_end = (get_time).replace(second=0, microsecond=0)

    # log.info(f"filter_start: {filter_start.strftime('%Y-%m-%dT%H:%M:%S.%f%z')}")
    # log.info(f"filter_end: {filter_end.strftime('%Y-%m-%dT%H:%M:%S.%f%z')}")

    # ti.xcom_push(key='filter_start', value=filter_start.strftime('%Y-%m-%dT%H:%M:%S.%f%z'))
    # ti.xcom_push(key='filter_end', value=filter_end.strftime('%Y-%m-%dT%H:%M:%S.%f%z'))

python_task = PythonOperator(
    task_id='python_task',
    python_callable=python_function,
    provide_context=True,  # This provides 'ti' to the function
    dag=dag
)

# query_task = PostgresOperator(
#     task_id='postgres_task',
#     postgres_conn_id='visee_postgres',
#     sql="""
#         SELECT
#             %(filter_start)s AT TIME ZONE 'Asia/Bangkok' as field_1,
#             %(filter_end)s AT TIME ZONE 'Asia/Bangkok' as field_2,
#             id
#         FROM bak_visitor
#         LIMIT 5;
#     """,
#     parameters={
#         'filter_start': '{{ ti.xcom_pull(task_ids="python_task", key="filter_start") }}',
#         'filter_end': '{{ ti.xcom_pull(task_ids="python_task", key="filter_end") }}'
#     },
#     dag=dag
# )

query_operator = SQLExecuteQueryOperator(
    task_id='sql_task',
    # conn_id=visee_postgres,
    conn_id='postgre_local',
    sql='sql/to_monitor_peak.sql',
    parameters={
        'filter_start': '{{ ti.xcom_pull(task_ids="python_task", key="filter_start") }}',
        'filter_end': '{{ ti.xcom_pull(task_ids="python_task", key="filter_end") }}'
    },
    dag=dag
)

# start >> bash_task >> python_task >> query_task >> end
start >> bash_task >> python_task >> query_operator >> end
