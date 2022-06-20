from datetime import datetime, timedelta
from airflow import models


default_dag_args = {
    'depends_on_past' : False,
    'email' : 'jono.fields@gmail.com',
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1),
    'start_date' : datetime(2022,6,21),
    'project_id' : 'pipeline-builds'
    
}

with models.DAG(
    'arrests_pipeline',
    schedule_interval=timedelta(weeks=1),
    default_args = default_dag_args) as dag:
    
    from airflow.operators import bash_operator
    from airflow.operators import python_operator
    import sys
    import os
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(os.path.dirname(SCRIPT_DIR))

    from dags_lib.crime_object import upload_file_arrests

    get_data_arrests = bash_operator.BashOperator(
        task_id = 'chicago_arrests',
        bash_command = """curl -o "/home/airflow/gcs/data/chicago_arrests.csv" -X GET https://data.cityofchicago.org/resource/dpt3-jri9.csv"""
    )


    arrests_to_storage = python_operator.PythonOperator(
        task_id = 'arrests_to_storage',
        python_callable = upload_file_arrests
    )


    arrests_to_bq = bash_operator.BashOperator(
        task_id = 'arrests_bq',
        bash_command = "bq load --autodetect --source_format=CSV --allow_quoted_newlines chicago_data.arrests gs://us-central1-test-run-ner-da18cc30-bucket/data/chicago_arrests.csv"
    )


    get_data_arrests >> arrests_to_storage >> arrests_to_bq