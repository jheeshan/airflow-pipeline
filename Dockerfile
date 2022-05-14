FROM apache/airflow:2.2.4

COPY requirements.txt .

COPY pipeline.conf .

RUN pip install --upgrade pip

RUN pip install --no-cache-dir apache-airflow-providers-mongo apache-airflow-providers-postgres apache-airflow-providers-amazon s3fs
