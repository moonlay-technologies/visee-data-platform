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
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLTableCheckOperator,
    SQLCheckOperator,
)
# from airflow.operators.check_operator import CheckOperator
from airflow.operators.email_operator import EmailOperator

import logging

log = logging.getLogger(__name__)

param = Variable.get("test_var", deserialize_json=True)
schedule_interval = param["schedule_interval"]
date_params = param["date_params"]
env =  Variable.get("visee_credential", deserialize_json=True)
postgres_local = env["postgres_local_url"]
postgres_visee = env["postgres_visee"]
local_tz = pendulum.timezone("Asia/Jakarta")
date_today = datetime.now()
email_reciver = ['ivan.rivaldo@moonlay.com']


args = {
    'owner': 'Ivan',
    'start_date': datetime(2024, 1, 13, tzinfo=local_tz),
    'retries': 2,
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id='dag_test',
    default_args=args,
    description='Test DAG',
    # schedule_interval=schedule_interval,
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


# -----------------------||------------------------
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
    filter_start = '2024-02-21T06:00:00' 
    filter_end = '2024-02-21T23:59:59'
    filter_date = '2024-02-21' 

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

def send_error_email(context):
    buss_date = period_string
    table_name = context['params']['table_name']
    print("Sending Error Email for {0} in buss_date {1}".format(table_name,buss_date))
    email_op = EmailOperator(
        task_id='send_email',
        to=email_receiver,
        subject="[Error] ETL {0} is NOT generating any data".format(dag.description),
        html_content="""<h3>Table {0} for Business Date {1} is Empty, please check DAG {2}</h3>""".format(table_name, buss_date, dag.description)
    )
    email_op.execute(context)
    print("Error email sent !")

# CheckOperator = CheckOperator(
#     task_id='check_table',
#     conn_id='visee_postgres',
#     table_name = 'history_state',
#     params = {
#         'table_name':table_name,
#         'filter_date': '{{ ti.xcom_pull(task_ids="task_get_filter", key="filter_date") }}'
#     },
#     sql=f"""select count(1) from {table_name} where "date" = %(filter_date)s """,
#     on_failure_callback=send_error_email,
#     dag=dag
# )

# CheckOperator = SQLCheckOperator(
#     task_id='check_table',
#     conn_id='visee_postgres',
#     sql=f"""select count(1) from history_state where "date" = %(filter_date)s""",
#     parameters={'filter_date': '{{ ti.xcom_pull(task_ids="task_get_filter", key="filter_date") }}'},
#     dag=dag
# )

CheckOperator = SQLTableCheckOperator(
    task_id='check_table',
    conn_id='visee_postgres',
    table='history_state',
    partition_clause = """ "date" = '{{params.filter_date}}' """,
    params={
    'filter_date': "{{ ti.xcom_pull(task_ids='task_get_filter', key='filter_date') }}"
    },
    checks={
        "table_check_count" : {
            "check_statement" : {"COUNT(1) >= 1"},
            # "partition_clause" : {" date = '{{params.filter_date}}' "}
        },
    },
    dag=dag
)



# start >> bash_task >> python_task >> query_task >> end
start >> get_filter >> CheckOperator >> end
