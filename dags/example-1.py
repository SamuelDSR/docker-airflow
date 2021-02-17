from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.helpers import chain, cross_downstream

from random import seed, randint
import time
import json

from datetime import timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(10),
    "sla": timedelta(hours=1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG("airflow-workshop-1", catchup=True, default_args=default_args, schedule_interval="00 01 * * *")

bash_task = BashOperator(
    task_id="bash_welcome",
    bash_command='echo "hello $name, today is `date`"',
    env={"name": "newbees"},
    dag=dag
)


def generate_random_numbers(max):
    seed(time.time_ns() % 1000)
    count = randint(0, max)
    print("successfully generate {} random numbers".format(count))


python_task = PythonOperator(
    task_id="python_generate_log",
    python_callable=generate_random_numbers,
    op_args=[1000], dag=dag
)

bash_task >> python_task
