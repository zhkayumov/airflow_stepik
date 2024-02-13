from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import sqlite3

con = sqlite3.connect('sqlite3.db')
cursor = con.cursor()

def extract_data(url, tmp_file):
    pd.read_csv(url).to_csv(tmp_file)

def merge_data(data1, data2, key, tmp_file):
  data1 = pd.read_csv(data1)
  data2 = pd.read_csv(data2)
  data = data1.merge(data2,  left_on = key, right_on = key)
  data.to_csv(tmp_file)

def insert_to_db(data):
  data = pd.read_csv(data)
  data.to_sql('table', con=con, if_exists="append", index=False)


with DAG(dag_id = 'dag_for_ex_2',
          default_args={'owner': 'airflow'},
          schedule_interval = timedelta(days=1),
          start_date = days_ago(1)
) as dag:

    extract_exchange = PythonOperator(
        task_id = 'extract_exchange',
        python_callable = extract_data,
        op_kwargs = {
            'url' : 'https://raw.githubusercontent.com/datanlnja/airflow_course/main/excangerate/2021-01-01.csv',
            'tmp_file' : 'exchange.csv'}
    )

    extract_data_file = PythonOperator(
        task_id = 'extract_data_file',
        python_callable = extract_data,
        op_kwargs = {
            'url' : 'https://raw.githubusercontent.com/datanlnja/airflow_course/main/data/2021-01-01.csv',
            'tmp_file' : 'data.csv'}
    )

    merge_data = PythonOperator(
        task_id = 'merge_data',
        python_callable = merge_data,
        op_kwargs = {
            'data1' : 'exchange.csv',
            'data2' : 'data.csv',
            'key' : 'date',
            'tmp_file' : 'new_data.csv'}
    )

    insert_to_db = PythonOperator(
        task_id = 'insert_to_db',
        python_callable = insert_to_db,
        op_kwargs = {
            'data' : 'new_data.csv'}
    )

    extract_exchange >> extract_data_file #>> merge_data >> insert_to_db


