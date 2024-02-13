from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

from sqlalchemy import create_engine
import pandas as pd

# Я делаю задание через докер на локальной машине - у меня уже в образе используется БД Postgres
engine = create_engine("postgresql://airflow:airflow@host.docker.internal:5432/airflow")

# Скачиваем валюту и добавляем в Xcom значение
def extract_currency(date, **kwargs):
    url = f'https://raw.githubusercontent.com/datanlnja/airflow_course/main/excangerate/{date}.csv'
    data = pd.read_csv(url, index_col=False)
    kwargs['task_instance'].xcom_push(key='amount', value=data['amount'].values[0])

# Скачиваем даныне и пишем в базу
def insert_to_db(data, table_name):
    conn = engine.connect()
    data.to_sql(table_name, conn, schema='my_schema_for_ex', if_exists='append', index=False)
    conn.close

def extract_data(date, conn):
    url = f'https://raw.githubusercontent.com/dm-novikov/stepik_airflow_course/main/data_new/{date}.csv'
    data = pd.read_csv(url, index_col=False)
    insert_to_db(data, 'data')

with DAG(dag_id = 'dag_for_ex_5',
    default_args = {'owner': 'airflow'},
    start_date=datetime(2021, 1, 1),
    end_date=datetime(2021, 1, 4),
    schedule_interval = '0 0 * * *',
    max_active_runs = 1
     ) as dag:

    # Операторы
    read_csv_file_1 = PythonOperator(task_id='read_currency',
                                     python_callable=extract_currency,
                                     op_kwargs={'date': '{{ ds }}'}
                                     )

    read_csv_file_2 = PythonOperator(task_id='read_data',
                                     python_callable=extract_data,
                                     op_kwargs={'date': '{{ ds }}'},
                                     )

    join_data = PostgresOperator(task_id='join_data',
                                postgres_conn_id='postgres_localhost',
                                sql="""
                                    set search_path to my_schema_for_ex;
                                    insert into new_data 
                                    select *, {{ task_instance.xcom_pull(task_ids='read_currency', key = 'amount') }}::float as amount
                                    from data;
                                    """
                                )

    [read_csv_file_1, read_csv_file_2] >> join_data