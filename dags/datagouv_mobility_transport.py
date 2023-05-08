from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd
import requests
import io
import json, csv


default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 5, 8),
}

dag = DAG(
    'datagouv_mobility_transport',
    default_args=default_args,
    description='Data pipeline for mobility and transport datasets from data.gouv.fr',
    schedule_interval='0 17 * * 3',  # Runs every Wednesday at 5 PM
    catchup=False,
)

def fetch_and_filter_datasets():
    url = "https://www.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3"
    r = requests.get(url)
    df = pd.read_csv(io.StringIO(r.text), sep=';', on_bad_lines='skip', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    print(df.columns)


    filtered_df = df[df['title'].str.contains("mobilité|transport", case=False, na=False)]
    return filtered_df

#Export the filtered list to MinIO bucke
def export_to_minio(filtered_df):
    date_str = datetime.now().strftime('%Y-%m-%d')
    csv_buffer = io.StringIO()
    filtered_df.to_csv(csv_buffer, index=False)
    s3_hook = S3Hook(aws_conn_id='minio_connection')

    # Create the bucket if it doesn't exist
    bucket_name = 'datagouv'
    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name)

    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        bucket_name=bucket_name,
        key=f'{date_str}/filtered_datasets.csv',
        replace=True  # Set replace to True
    )

    
def filter_by_popularity(filtered_df):
    url = "https://www.data.gouv.fr/fr/datasets/r/b7bbfedc-2448-4135-a6c7-104548d396e7"
    r = requests.get(url)
    print(filtered_df.columns)
    org_df = pd.read_csv(io.StringIO(r.text), delimiter=";", quotechar='"', error_bad_lines=False)
    print(org_df.columns)
    org_df = org_df.nlargest(30, 'metric.followers')

    popular_datasets = filtered_df.merge(org_df, left_on='organization_id', right_on='id')
    popular_datasets = popular_datasets[[
        'id_x', 'title', 'metric.followers_x', 'organization',
        'organization_id', 'metric.followers_y'
    ]]

    return popular_datasets



#Export to a MongoDB document
def export_to_mongo(popular_datasets):
    mongo_hook = MongoHook(conn_id='mongo_connection')
    mongo_hook.insert_many(
        mongo_db='datagouv',
        mongo_collection='tops',
        docs=popular_datasets.to_dict('records'),
    )


#Create the PythonOperator tasks and set their dependencies
def run_pipeline():
    filtered_df = fetch_and_filter_datasets()
    export_to_minio(filtered_df)
    popular_datasets = filter_by_popularity(filtered_df)
    export_to_mongo(popular_datasets)

run_pipeline_task = PythonOperator(
    task_id='run_pipeline',
    python_callable=run_pipeline,
    dag=dag,
)

run_pipeline_task
