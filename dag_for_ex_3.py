from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def my_func(hello, date, **context):
  print(hello)
  print(date)
  print(context["dag"])


with DAG(dag_id = 'dag_for_ex_3',
          default_args={'owner': 'airflow'},
          schedule_interval='@daily',
          start_date= datetime(2020, 9, 19),
#          end_date=datetime(2021, 1, 10)
         ) as dag:

  python_task = PythonOperator(
    task_id='python_task',
    python_callable=my_func,
    op_kwargs= {
      'hello': 'Hello World',
      'date': '{{ execution_date }}'
      }
    )
