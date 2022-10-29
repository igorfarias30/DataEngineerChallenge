from email.policy import default
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from helpers.etl import my_funcion

default_args = {
    'owner': 'Igor Farias',
    'depends_on_past': False,
    'start_date': days_ago(1)
}

with DAG(
    'ingestion_trip',
    default_args=default_args,
    schedule_interval=None
) as dag:
    ingestion_task = PythonOperator(
        task_id = 'extract_data_from_csv',
        python_callable = my_funcion
    )

    ingestion_task