from __future__ import annotations

from datetime import timedelta, datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import pendulum

import pandas as pd
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

def python_function(date_params, **kwargs):
    print(f"This Python task uses params {date_params}")

python_task = PythonOperator(
    task_id='using_python_task',
    python_callable=python_function,
    provide_context=True,
    op_kwargs={'date_params': date_params},
    dag=dag
)

bash_task = BashOperator(
    task_id='using_bash_task',
    bash_command='echo "This Bash task uses params {}"'.format(date_params),
    dag=dag
)



start >> python_task >> bash_task >> end
# start >> bash_task >> end
