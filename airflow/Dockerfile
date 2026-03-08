FROM apache/airflow:3.1.7

USER airflow

RUN pip install psycopg[binary]
RUN pip install pymongo
RUN pip install google-cloud-BigQuery

USER airflow
