from datetime import datetime, timedelta
from airflow import models


default_dag_args = {
    'depends_on_past' : False,
    'email' : 'jono.fields@gmail.com',
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1),
    'start_date' : datetime(2022,6,20),
    'project_id' : 'pipeline-builds'
    
}

with models.DAG(
    'crime_pipeline',
    schedule_interval=timedelta(weeks=1),
    default_args = default_dag_args) as dag:
    
    from airflow.operators import bash_operator
    from airflow.operators import python_operator
    import sys
    import os


    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(os.path.dirname(SCRIPT_DIR))

    from dags_lib.crime_object import upload_file_crime


    get_data_crime = bash_operator.BashOperator(
        task_id = 'chicago_crime',
        bash_command = """curl -o "/home/airflow/gcs/data/chicago_crime.csv" -X GET https://data.cityofchicago.org/resource/ijzp-q8t2.csv"""
    )


    crime_to_storage = python_operator.PythonOperator(
        task_id = 'crime_to_storage',
        python_callable = upload_file_crime
    )


    crime_to_bq = bash_operator.BashOperator(
        task_id = 'crime_bq',
        bash_command = "bq load --autodetect --source_format=CSV --allow_quoted_newlines chicago_data.crime gs://us-central1-test-run-ner-da18cc30-bucket/data/chicago_crime.csv"
    )


    

    

    get_data_crime >> crime_to_storage >>  crime_to_bq


#budgets:
    # 2011 - https://data.cityofchicago.org/resource/drv3-jzqp.csv
    # 2012 - https://data.cityofchicago.org/resource/8ix6-nb7q.csv
    # 2013 - https://data.cityofchicago.org/resource/b24i-nwag.csv
    # 2014 - https://data.cityofchicago.org/resource/ub6s-xy6e.csv
    # 2015 - https://data.cityofchicago.org/resource/qnek-cfpp.csv
    # 2016 - https://data.cityofchicago.org/resource/36y7-5nnf.csv
    # 2017 - https://data.cityofchicago.org/resource/7jem-9wyw.csv
    # 2018 - https://data.cityofchicago.org/resource/6g7p-xnsy.csv
    # 2019 - https://data.cityofchicago.org/resource/h9rt-tsn7.csv
    # 2020 - https://data.cityofchicago.org/resource/fyin-2vyd.csv
    # 2021 - https://data.cityofchicago.org/resource/6tbx-h7y2.csv

