from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow import AirflowException
from airflow.providers.telegram.operators.telegram import TelegramOperator

def fail_func():
     raise AirflowException

def fail_func_to_do(context):
    send_message = TelegramOperator(
        task_id='send_message_telegram',
        token = '6841422997:AAFykke79bWtkDnaYeS8HyAEANRJWC4CnOc',
        chat_id = '-4196016669',
        text='Check airflow dag'
    )
    return send_message.execute(context=context)

default_args = {
     'owner': 'airflow',
    'on_failure_callback': fail_func_to_do
}

with DAG(dag_id = 'dag_for_ex_6',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '0 0 * * *'
     ) as dag:

     task_1 = PythonOperator(
          task_id = 'fail_func',
          python_callable = fail_func
     )


     task_1