from datetime import datetime, timedelta
from airflow import models


default_dag_args = {
    'depends_on_past' : False,
    'email' : 'jono.fields@gmail.com',
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1),
    'start_date' : datetime(2022,6,14),
    'project_id' : 'pipeline-builds'
    
}

with models.DAG(
    'crime_pipeline',
    schedule_interval=timedelta(weeks=1),
    default_args = default_dag_args) as dag:
    
    from airflow.operators import bash_operator
    from airflow.operators import python_operator
    from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
    import sys
    import os


    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(os.path.dirname(SCRIPT_DIR))

    from dags_lib.crime_object import upload_file_crime, upload_file_arrests


    get_data_crime = bash_operator.BashOperator(
        task_id = 'chicago_crime',
        bash_command = """curl -o "/home/airflow/gcs/data/chicago_crime.csv" -X GET https://data.cityofchicago.org/resource/ijzp-q8t2.csv"""
    )


    crime_to_storage = python_operator.PythonOperator(
        task_id = 'crime_to_storage',
        python_callable = upload_file_crime
    )


    get_data_arrests = bash_operator.BashOperator(
        task_id = 'chicago_arrests',
        bash_command = """curl -o "/home/airflow/gcs/data/chicago_arrests.csv" -X GET https://data.cityofchicago.org/resource/dpt3-jri9.csv"""
    )


    arrests_to_storage = python_operator.PythonOperator(
        task_id = 'arrests_to_storage',
        python_callable = upload_file_arrests
    )

    crime_to_bq = bash_operator.BashOperator(
        task_id = 'crime_bq',
        bash_command = "bq load --autodetect --source_format=CSV --allow_quoted_newlines chicago_data.crime gs://us-central1-test-run-ner-da18cc30-bucket/data/chicago_crime.csv"
    )

    arrests_to_bq = bash_operator.BashOperator(
        task_id = 'arrests_bq',
        bash_command = "bq load --autodetect --source_format=CSV --allow_quoted_newlines chicago_data.arrests gs://us-central1-test-run-ner-da18cc30-bucket/data/chicago_arrests.csv"
    )

    

    get_data_crime >> crime_to_storage >> get_data_arrests >> arrests_to_storage >> crime_to_bq >> arrests_to_bq


