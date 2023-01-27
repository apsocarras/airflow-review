from datetime import timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago 

import random as rd 
import os

APPLES = ["pink lady", "jazz", 
"orange pippin", "granny smith", "red delicious", 
"gala", "honeycrisp", "mcintosh", "fuji"]

def print_hello():
  """Open code_review.txt, read name from file, and print a greeting."""
  path = os.path.abspath(__file__)
  dir_name = os.path.dirname(path)
  with open(f"{dir_name}/code_review.txt", "r") as file: 
    name = file.read().strip()

  print(f"Hello, {name}!")

def random_apple():
  """Print a randomly-selected apple variety from APPLES"""
  print(rd.choice(APPLES))

default_args = {
    'start_date': days_ago(2), 
    'schedule_interval': timedelta(days=1), 
    'retries': 1, 
    'retry_delay': timedelta(seconds=3), 
}

with DAG(
  'code_review',
  description='Code review for Chapter 11',
  default_args=default_args
) as dag:

  echo_to_file = BashOperator(
    task_id="echo_file", 
    bash_command=f'echo "Alejandro Socarras" > /opt/airflow/dags/code_review.txt'
  )

  greeting = PythonOperator(
    task_id="print_greeting", 
    python_callable=print_hello
  )

  rand_apples = BashOperator(
    task_id="rand_apples", 
    bash_command="echo 'picking three random apples'"
  )

  apple_tasks = []
  for i in range(3): 
    task=PythonOperator(
      task_id=f"apple_{i}", 
      python_callable=random_apple
    )
    apple_tasks.append(task)

  end_task = EmptyOperator(task_id="end")

  echo_to_file >> greeting >> rand_apples >> apple_tasks >> end_task