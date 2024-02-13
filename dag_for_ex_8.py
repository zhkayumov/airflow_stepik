from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

default_args = {
     'owner': 'airflow'
}

with DAG(dag_id = 'dag_for_ex_8',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '0 0 * * *'
     ) as dag:

    task_1 = BashOperator(
        task_id='clean_logs',
        bash_command = 'rm -i logs/*',
    )

    task_1