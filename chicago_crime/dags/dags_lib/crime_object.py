from datetime import date
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
import os

def upload_file_crime():  
    conn = GoogleCloudStorageHook()
    date_td = date.today()

    bucket_name = 'data_for_hobby_pipelines'
    file_name = '/home/airflow/gcs/data/chicago_crime.csv' 
    object_name = f'{date_td}_crime.csv'
    conn.upload(bucket_name,object_name,file_name)
            
def upload_file_arrests():
    conn = GoogleCloudStorageHook()
    date_td = date.today()
    
    bucket_name = 'data_for_hobby_pipelines'
    file_name = '/home/airflow/gcs/data/chicago_arrests.csv' 
    object_name = f'{date_td}_arrests.csv'
    conn.upload(bucket_name,object_name,file_name)