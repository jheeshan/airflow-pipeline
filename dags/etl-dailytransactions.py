import configparser
import csv
import datetime
import json

import airflow.utils.dates
import numpy as np
import pandas as pd
import psycopg2
import s3fs
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient

from common import get_last_char

# load variables
target_collection = 'dailytransactions' # name of collection in MongoDB
date = datetime.date.today()            # daily task, so we pull newest data each day

parser = configparser.ConfigParser()
parser.read("pipeline.conf")

# load mongodb details
mongo_hostname = parser.get("mongo_config", "hostname")
mongo_username = parser.get("mongo_config", "username")
mongo_password = parser.get("mongo_config", "password")
mongo_database_name = parser.get("mongo_config", "database")
collection = parser.get("mongo_config", f"collection_{target_collection}")

# load postgres db details
postgres_host = parser.get("postgres_db", "host")
postgres_username = parser.get("postgres_db", "username")
postgres_password = parser.get("postgres_db", "password")
postgres_dbname = parser.get("postgres_db", "database")
postgres_port = parser.get("postgres_db", "port")

# load s3 bucket details
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

# list of clients with validated data to extract from
valid_clients = ['A', 'B', 'C', 'D', 'E']


# connect to MongoDB and pull documents that match the validated list
# we only pull the fields needed for analytics
# convert into DataFrame and output a CSV
# push the CSV to S3 for staging and cleaning
def get_dailytransactions_data():
    mongo_client = MongoClient(
        f"mongodb://{mongo_username}:{mongo_password}"
        f"@{mongo_hostname}/{mongo_database_name}?retryWrites=true&w=majority"
    )
    mongo_db = mongo_client[mongo_database_name]
    mongo_collection = mongo_db[collection]
    mongo_query = {"clientId": {"$in": valid_clients}}
    event_docs = mongo_collection.find(mongo_query, batch_size=1000)

    all_events = []

    for doc in event_docs:
        # Include default values. Add -1 or None as arg to account for cases
        # where a document does not contain that field
        event_id = str(doc.get("_id", None))
        owner_id = str(doc.get("ownerId", None))
        client_id = str(doc.get("clientId", None))
        event_timestamp = doc.get("currentTime", None)
        metric = doc.get("metric", None)
        metric2 = doc.get("metric2", None)
        metric3 = doc.get("metric3", None)

        # add all the event properties into a list
        current_event = []
        current_event.append(event_id)
        current_event.append(owner_id)
        current_event.append(client_id)
        current_event.append(event_timestamp)
        current_event.append(metric)
        current_event.append(metric2)
        current_event.append(metric3)

        all_events.append(current_event)

    df = pd.DataFrame(all_events)
    file_writen_name = f'dailytransactions_{date}.csv'

    fs = s3fs.S3FileSystem(key=access_key, secret=secret_key)
    with fs.open(
        f's3://{bucket_name}/mongodb-extract/raw/{file_writen_name}', 'w'
    ) as f:
        df.to_csv(f, sep='|', index=False, header=False)
    f.close()


# transform task. We convert a datetime column, json column to a CSV friendly format
# and extract relevant substrings from certain columns
def transform_dailytransactions_data():
    file_to_clean = f'dailytransactions_{date}.csv'
    src_path = f's3://{bucket_name}/mongodb-extract/raw/{file_to_clean}'

    fs = s3fs.S3FileSystem(key=access_key, secret=secret_key)
    with fs.open(src_path, 'r') as f:
        df = pd.read_csv(f, sep='|', header=None)
    f.close()

    df[3] = pd.to_datetime(df[3])                  # validate datetimes
    df[4] = df[4].apply(json.dumps)                # convert a column that is in json array format
    df[4].replace("\"[]\"", np.nan, inplace=True)    
    df[6] = df[6].map(get_last_char)               # column is in format "X-Y-Z" and we only want Z for analytics
    
    cleaned_file = f'dailytransactions_cleaned_{date}.csv'

    with fs.open(
        f's3://{bucket_name}/mongodb-extract/processed/{cleaned_file}', 'w'
    ) as f:
        df.to_csv(f, sep='\t', quoting=csv.QUOTE_NONE,
                  index=False, header=False)
    f.close()


# load task. We truncate the table in our Postgres instance and load it.
# This is a backup table. Only after further validation is done in SQL
# do we complete the check and move the data to the analytics table.
def load_into_postgres():
    postgres_conn = psycopg2.connect(
        f"dbname={postgres_dbname} user={postgres_username} "
        f"password={postgres_password} host={postgres_host} "
        f"port={postgres_port}"
    )
    cursor = postgres_conn.cursor()
    cursor.execute('TRUNCATE TABLE public.dailytransactions')
    target_file = f'dailytransactions_cleaned_{date}.csv'
    file_to_copy = (
        f's3://{bucket_name}/mongodb-extract/processed/{target_file}'
    )
    fs = s3fs.S3FileSystem(key=access_key, secret=secret_key)
    with fs.open(file_to_copy, 'r') as dailytransactions:
        cursor.copy_from(
            file=dailytransactions,
            table='dailytransactions',
            sep='\t',
            null=''
        )

    cursor.close()
    postgres_conn.commit()
    postgres_conn.close()


default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'schedule_interval': 'daily'
}

with DAG(
    dag_id='etl-dailytransactions-daily_v1.0.1',
    default_args=default_args
) as dag:

    extraction_task = PythonOperator(
        task_id='download_dailytransactions_task',
        python_callable=get_dailytransactions_data
    )

    clean_task = PythonOperator(
        task_id='clean_dailytransactions_data',
        python_callable=transform_dailytransactions_data
    )

    load_task = PythonOperator(
        task_id='load_into_postgres',
        python_callable=load_into_postgres
    )

    extraction_task >> clean_task >> load_task
