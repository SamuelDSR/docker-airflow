from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.helpers import chain, cross_downstream

from random import seed, randint
import time
import json
import os

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

dag = DAG("airflow-workshop-2", catchup=False, default_args=default_args, schedule_interval="*/5 * * * *")

bash_task = BashOperator(
    task_id="bash_welcome",
    bash_command='echo "hello $name, today is $today"',
    env={"name": "newbees", "today": "{{ ds }}"},
    dag=dag
)


def generate_random_logs(max, log_dir):
    seed(time.time_ns() % 1000)
    count = randint(0, 1000)
    numbers = [randint(0, max) for x in range(count)]
    os.makedirs(log_dir, exist_ok=True)
    log_path = log_dir + "/log.json"
    json.dump(numbers, open(log_path, "w"))
    print("successfully generate {} random numbers and saved to {}".format(count, log_path))


def sum_logs(log_dir):
    log_path = log_dir + "/log.json"
    with open(log_path, "r") as f:
        numbers = json.load(f)
        print("The sum of numbers is {}".format(sum(numbers)))


python_task = PythonOperator(
    task_id="python_generate_log",
    python_callable=generate_random_logs,
    op_kwargs={"max": 1000, "log_dir": "/data/" + "{{ execution_date.strftime('%Y/%m/%d/%H/%M') }}"},
    dag=dag
)

sum_task = PythonOperator(
    task_id="python_sum_log",
    python_callable=sum_logs,
    op_args=["/data/" + "{{ execution_date.strftime('%Y/%m/%d/%H/%M') }}"],
    dag=dag
)

bash_task >> python_task >> sum_task
