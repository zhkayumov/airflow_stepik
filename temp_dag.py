from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

def print_context():
    return 'Hello World'

default_args = {
     'owner': 'airflow'
}

with DAG(dag_id = 'temp_dag',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '0 0 * * *'
     ) as dag:

    task_1 = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context,
        dag=dag,
    )

    task_1