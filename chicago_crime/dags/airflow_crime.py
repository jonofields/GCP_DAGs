from datetime import datetime, timedelta

from airflow import models



default_dag_args = {
    'depends_on_past' : False,
    'email' : 'jono.fields@gmail.com',
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
    'start_date' : datetime(2022,6,14),
    'project_id' : 'pipeline-builds'
    
}

with models.DAG(
    'crime_pipeline',
    schedule_interval=timedelta(days=3),
    default_args = default_dag_args) as dag:
    
    from airflow.operators import bash_operator
    from airflow.operators import python_operator
    from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
    import sys
    import os

    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(os.path.dirname(SCRIPT_DIR))

    from dags_lib.crime_object import upload_file

    get_data = bash_operator.BashOperator(
        task_id = 'chicago_crime',
        bash_command = """curl -o "/home/airflow/gcs/data/chicago_crime.csv" -X GET https://data.cityofchicago.org/resource/ijzp-q8t2.csv"""
    )


    data_to_storage = python_operator.PythonOperator(
        task_id = 'crime_to_storage',
        python_callable = upload_file
    )

    get_data >> data_to_storage


