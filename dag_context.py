from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

dag = DAG('dag_context',schedule_interval=timedelta(days=1), start_date=days_ago(1))

def print_context(**context):
    context['ti'].xcom_push(key='context_len', value=str(context))

run_this = PythonOperator(
    task_id='print_the_context',
    python_callable=print_context,
    dag=dag,
)