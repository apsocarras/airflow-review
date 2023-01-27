from datetime import timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago 

import pandas as pd 
import os

APPLES = ["pink lady", "jazz", 
"orange pippin", "granny smith", "red delicious", 
"gala", "honeycrisp", "mcintosh", "fuji"]

def print_hello():
  """Open code_review.txt, read name from file, and print a greeting."""
  path = os.path.abspath(__file__)
  dir_name = os.path.dirname(path)
  with open(f"{dir_name}/code_review.txt", "r") as file: 
    name = file.read()

  print(f"Hello, {name}!")

default_args = {
    'start_date': days_ago(2), 
    'schedule_interval': timedelta(days=1), 
    'retries': 1, 
    'retry_delay': timedelta(seconds=10), 
}

with DAG(
  'code_review',
  description='Read/transform India_Menu.csv and write summary metrics to JSON',
  default_args=default_args
) as dag:


