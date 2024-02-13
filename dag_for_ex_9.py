from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator

default_args = {
     'owner': 'airflow'
}

with DAG(dag_id = 'dag_for_ex_9',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '0 0 * * *'
     ) as dag:

    task_1 = SimpleHttpOperator(
        task_id = 'check_random',
        method = 'GET',
        http_conn_id = 'random_api',
        endpoint = '/integers/?num=1&min=1&max=5&col=1&base=2&format=plain'
    )

    task_1