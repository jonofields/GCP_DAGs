from datetime import date
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

def upload_file():
        
    conn = GoogleCloudStorageHook()
    date_td = date.today()
    bucket_name = 'data_for_hobby_pipelines'
    object_name = f'{date_td}.csv'
    file_name = '/home/airflow/gcs/data/chicago_crime.csv' 

    conn.upload(bucket_name,object_name,file_name)
    