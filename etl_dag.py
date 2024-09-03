from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import pandas as pd
from data_extract import data_extract
from clean_data import clean_data
from transform_data import transform_data
from insert_data import insert_data



# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 1),
    'retries': 0,
}
#DAG
dag = DAG(
    'etl_dag',
    default_args=default_args,
    schedule_interval=timedelta(hours=3),  # Set the schedule as needed
    catchup=False,
)
#Task for data extraction
data_extract_task = PythonOperator(
    task_id='data_extract',
    python_callable=data_extract,
    retries=1,
    retry_delay=timedelta(seconds=20),
    provide_context=True,
    dag=dag,
)

#Task for Data cleaning
clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    retries=1,
    retry_delay=timedelta(seconds=20),
    provide_context=True,
    dag=dag,
)

#Task for Transforming data
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    retries=1,
    retry_delay=timedelta(seconds=20),
    provide_context=True,
    dag=dag,
)

#Task for inserting data in Mongodb
insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    provide_context=True,
    dag=dag,
)

data_extract_task>>clean_data_task>>transform_data_task>>insert_data_task
