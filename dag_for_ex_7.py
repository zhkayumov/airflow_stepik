from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from random import randint

def rand(**kwargs):
  kwargs['ti'].xcom_push(key='rand', value=randint(0, 10))

def branch(**kwargs):
    xcom_value = int(kwargs['ti'].xcom_pull(task_ids='random_number', key = 'rand'))
    if xcom_value > 5:
        return 'higher'
    else:
        return 'lower'

default_args = {
     'owner': 'airflow'
}

with DAG(dag_id = 'dag_for_ex_7',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '0 0 * * *'
     ) as dag:

    lower = DummyOperator(
        task_id = 'lower'
    )

    higher = DummyOperator(
        task_id = 'higher'
    )

    branch_op = BranchPythonOperator(
        task_id = 'branch_task',
        provide_context = True,
        python_callable=branch
    )

    random_number = PythonOperator(
        task_id = 'random_number',
        python_callable=rand
    )

    random_number >> branch_op >> [lower, higher]