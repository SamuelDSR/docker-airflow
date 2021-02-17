# Recsys workshop on airflow
[TOC]
## Introduction
### What's airflow
Airflow is a workflow management platform, which allow users to create complex data pipeline by writting task as DAGs. 
It provides rich utilities to scheduler, visualize, monitoring and troubleshoot your pipeline.
Airflow suites perfectly for ETL batch pipeline.

> What's DAG: stands for **Directed Acyclic Graphs**, a directed graphs without cycles.

### Why airflow?
Benefits of airflow:
- The concept of DAG is nice abstraction to specify dependencies between tasks.
- DAG are constructed programatically by code, written in pure **Python**.
- A nice UI interface for monitoring and management.
- Very extensible: a lot of community contributed operators for aws, gcloud, azure, you can write your own easily too.

### How airflow works
[Airflow architecture](https://airflow.apache.org/docs/apache-airflow/stable/_images/arch-diag-basic.png)
There are several components that are necessary for airflow to work.
- **Metadata database**: to store dags, variables, credentials, status of tasks, etc. Any SQL database will work (even sqlite), recommend postgresql.
- **Scheduler**: run your dags at the specified time. (Like the cron services in linux).
- **Executor**: a queue that holds the scheduled tasks (by default, a Sequential executor),  but we could use also other advanced task queue such as celery.
- **Worker**: a process spawn by sceduler/executor to do the workload.
- **DAG**: a collection of tasks, written in python but not pure python script. The airflow scheduler will dynamically render and parser him to get the actual DAG representation.

## Deployment your first airflow server
Because airflow depends on several components to be preinstalled. To avoid the 
tedious works of installing, we will demonstrate using `docker-composer` to facilitate this process.

### Install docker and docker-compose
- For macos/windows users: install Docker Destop, if you already have docker, then docker compose is already integrated.
- For linux users: follow this guide [here](https://docs.docker.com/compose/install/#install-compose-on-linux-systems)

### Install airflow and components
- Clone this repo: [docker-airflow](https://github.com/SamuelDSR/docker-airflow.git): `git clone https://github.com/SamuelDSR/docker-airflow.git`
- Build the images: `docker build --rm --build-arg AIRFLOW_DEPS="gcp,gcp_api,kubernetes" --build-arg PYTHON_DEPS="SQLAlchemy=1.3.15" -t puckel/docker-airflow .`
- Install all components by: `docker-compose -f docker-compose-CeleryExecutor.yml up -d`
- Go to [airflow webserver](http://localhost:8080) for the airflow UI

### A tour of airflow UI interface
- DAG
  1. How to trigger a dag run
  2. How to relaunch some tasks

## Write you first airflow DAG
**Several concepts** to keep in mind when write dags:
- **Macros**: airflow provides several predefined macros when render the dags, see [here](https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html) for more details.
- Variables: to pass extra dag configs, two ways to refer the variables, `Variable.get('my_var')` or `var.value.my_var`
- Connection: information that airflow need to know to connect to external systems, such as metadatabase connection, executor connection.
- Operators: can only use operator to define task in DAG

### An minimal dag example of two dags.
```python
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
    "start_date": days_ago(2),
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


def gnenerate_random_logs(max):
    seed(time.time_ns() % 1000)
    count = randint(0, max)
    print("successfully generate {} random numbers".format(count))


python_task = PythonOperator(
    task_id="python_generate_log",
    python_callable=generate_random_logs,
    op_args=[1000], dag=dag
)

bash_task >> python_task
```
**Questions**:
1. What will be the output of operator `bash_task` when we do catchup? How can we output the correct date when the dag is scheduled.
2. What if we want to store the generated random numbers in `python_task` operator to a file so next we can calculate their sum using another operator?
3. What if we want to store these files in a cloud storage, such as gcloud?

### Practice
1. Change the scheduler from once per day to per 5 minutes
2. Suppose now we want to save the generate numbers into local disk, we can modify the function `generate_random_logs` as following:
```python
def generate_random_logs(max, log_dir):
    seed(time.time_ns() % 1000)
    count = randint(0, 1000)
    numbers = [randint(0, max) for x in range(count)]
    os.makedirs(log_dir, exist_ok=True)
    log_path = log_dir + "/log.json"
    json.dump(numbers, open(log_path, "w"))
    print("successfully generate {} random numbers and saved to {}".format(count, log_path))
```
Modify the python_task operator to save the daily logs in a format like /data/YYYY/MM/DD/HH/mm/log.json where `YYYY-MM-DD HH:mm:ss` represents the execution time.

- Now we want to sum the generated random numbers in previous step and we have the following python function to do the job
```python
def sum_logs(log_dir):
    log_path = log_dir + "/log.json"
    with open(log_path, "r") as f:
        numbers = json.load(f)
        print("The sum of numbers is {}".format(sum(numbers)))
```
Add an python operator to the dag to calculate the sum.
