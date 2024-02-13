import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta
import pandas as pd

def extract_data(url, date):
    url = url + '/' + str(date) + '.csv'
    result = pd.read_csv(url)
    return result['amount'].values[0]

with DAG(dag_id = 'dag_for_ex_4',
        default_args={'owner': 'airflow'},
        schedule_interval = timedelta(days=1),
#        start_date=days_ago(1)
        start_date = datetime(2021, 1, 1),
        end_date = datetime(2021, 1, 4)
) as dag:

    extract_exchange = PythonOperator(
        task_id='extract_exchange',
        python_callable=extract_data,
        op_kwargs={
            'url': 'https://raw.githubusercontent.com/datanlnja/airflow_course/main/excangerate',
            'tmp_file': 'exchange.csv',
            'date' : '{{ ds }}'}
    )


    extract_exchange

