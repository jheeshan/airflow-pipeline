import configparser
import datetime

import pandas as pd
import psycopg2
import s3fs
import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient

from common import get_last_char

# load variables
target_collection = 'dailyforecasts'
date = datetime.date.today()

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

# list of farms to extract from
valid_clients = ['A', 'B', 'C', 'D', 'E']


def get_dailyforecasts_data():
    mongo_client = MongoClient(
        f"mongodb://{mongo_username}:{mongo_password}"
        f"@{mongo_hostname}/{mongo_database_name}?retryWrites=true&w=majority"
    )
    mongo_db = mongo_client[mongo_database_name]
    mongo_collection = mongo_db[collection]
    mongo_query = {"farmId": {"$in": valid_clients}}
    event_docs = mongo_collection.find(mongo_query, batch_size=1000)

    all_events = []
    for doc in event_docs:
        # Include default values. Add -1 or None as arg to account for cases
        # where a document does not contain that field
        event_id = str(doc.get("_id", None))
        owner_id = str(doc.get("ownerId", None))
        client_id = str(doc.get("clientId", None))
        group_id = str(doc.get("groupId", None))
        forecast_task_id = str(doc.get("forecastTaskId", None))
        is_forecast = doc.get("isForecast", None)
        event_timestamp = doc.get("currentTime", None)
        age = doc.get("age", None)
        metric1 = doc.get("metric1", None)
        gain = doc.get("gain", None)
        metric2 = doc.get("metric2", None)
        metric3 = doc.get("metric3", None)

        # add all the event properties into a list
        current_event = []
        current_event.append(event_id)
        current_event.append(owner_id)
        current_event.append(client_id)
        current_event.append(group_id)
        current_event.append(forecast_task_id)
        current_event.append(is_forecast)
        current_event.append(event_timestamp)
        current_event.append(age)
        current_event.append(metric1)
        current_event.append(gain)
        current_event.append(metric2)
        current_event.append(metric3)

        all_events.append(current_event)

    df = pd.DataFrame(all_events)
    file_writen_name = f'dailyforecasts_{date}.csv'

    fs = s3fs.S3FileSystem(key=access_key, secret=secret_key)
    with fs.open(
        f's3://{bucket_name}/mongodb-extract/raw/{file_writen_name}', 'w'
    ) as f:
        df.to_csv(f, sep='|', index=False, header=False)
    f.close()


def transform_dailyforecasts_data():
    file_to_clean = f'dailyforecasts_{date}.csv'
    src_path = f's3://{bucket_name}/mongodb-extract/raw/{file_to_clean}'

    fs = s3fs.S3FileSystem(key=access_key, secret=secret_key)
    with fs.open(src_path, 'r') as f:
        df = pd.read_csv(f, sep='|', header=None)
    f.close()

    df[6] = pd.to_datetime(df[6])
    df[4] = df[4].map(get_last_char)
    
    cleaned_file = f'dailyforecasts_cleaned_{date}.csv'

    with fs.open(
        f's3://{bucket_name}/mongodb-extract/processed/{cleaned_file}', 'w'
    ) as f:
        df.to_csv(f, sep='|', index=False, header=False)
    f.close()


def load_into_postgres():
    postgres_conn = psycopg2.connect(
        f"dbname={postgres_dbname} user={postgres_username} "
        f"password={postgres_password} host={postgres_host} "
        f"port={postgres_port}"
    )
    cursor = postgres_conn.cursor()
    cursor.execute('TRUNCATE TABLE public.dailyforecasts')
    target_file = f'projections_cleaned_{date}.csv'
    file_to_copy = (
        f's3://{bucket_name}/mongodb-extract/processed/{target_file}'
    )
    fs = s3fs.S3FileSystem(key=access_key, secret=secret_key)
    with fs.open(file_to_copy, 'r') as dailyforecasts:
        cursor.copy_from(
            file=dailyforecasts,
            table='dailyforecasts',
            sep='|',
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
    dag_id='etl-dailyforecasts-daily_v1.0.2',
    default_args=default_args
) as dag:

    extraction_task = PythonOperator(
        task_id='download_dailyforecasts_task',
        python_callable=get_dailyforecasts_data
    )

    clean_task = PythonOperator(
        task_id='clean_dailyforecasts_data',
        python_callable=transform_dailyforecasts_data
    )

    load_task = PythonOperator(
        task_id='load_into_postgres',
        python_callable=load_into_postgres
    )

    extraction_task >> clean_task >> load_task
