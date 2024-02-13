# Используйте данный шаблон для решения
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException

dag = DAG('dag_for_ex_1',
          schedule_interval=timedelta(days=1),
          start_date=days_ago(1))


# Функция которая всегда верна
def success():
  pass

# Функция которая скипает задачу
def skip():
  raise AirflowSkipException

# Функция которая падает с ошибкой
def failed():
  raise AirflowFailException

task_4 = PythonOperator(
  task_id='task_4',
  python_callable=success,
  dag=dag
)

task_5 = PythonOperator(
  task_id='task_5',
  python_callable=failed,
  dag=dag
)

task_6 = PythonOperator(
  task_id='task_6',
  python_callable=failed,
  dag=dag
)

task_7 = PythonOperator(
  task_id='task_7',
  python_callable=lambda: print("Success"),
  trigger_rule='one_failed',
  dag=dag
)

[task_4, task_5,task_6] >> task_7
