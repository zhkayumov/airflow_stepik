from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook

def get_connection_settings():
    host = BaseHook.get_connection('custom_conn_id').host
    user = BaseHook.get_connection('custom_conn_id').login
    password = BaseHook.get_connection('custom_conn_id').password
    result = {'host': str(host), 'user': str(user), 'password': str(password)}
    return result

default_args = {
     'owner': 'airflow'
}

with DAG(dag_id = 'dag_for_ex_11',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '0 0 * * *'
     ) as dag:

    task_1 = PythonOperator(
        task_id='get_connection_settings',
        python_callable=get_connection_settings
    )

    task_1