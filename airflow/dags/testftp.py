from airflow import DAG
from airflow.providers.ftp.operators.ftp import FTPFileTransmitOperator, FTPOperation
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from pathlib import Path
import pandas as pd
import numpy as np
import psycopg as pgs
from sqlalchemy import create_engine
from pymongo import MongoClient
import json
from google.cloud import bigquery
from google.cloud.bigquery import Table
import os

default_args = {
    "retries": 1,
    "retry_delay": 5,
}

@dag(
    dag_id="test_ftp",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
    default_args=default_args,
)
def Test_Ftp():
    upload_task = FTPFileTransmitOperator(
        task_id="upload_to_ftp",
        ftp_conn_id="ftp_server",
        local_filepath="data/processed_data.csv",           # path in container met bind volume op host dat processed data file bevat
        remote_filepath="inbound/processed/data.csv",       # path op ftp server dat processed data zal ontvangen
        operation=FTPOperation.PUT,
        create_intermediate_dirs=False,
    )
    
    download_task = FTPFileTransmitOperator(
        task_id="download_from_ftp",
        ftp_conn_id="ftp_server",
        local_filepath="data/raw_data.csv",                 # path in container met bind volume waar raw data naar gedownload zal worden
        remote_filepath="outbound/raw/data.csv",            # path op ftp server met file die raw data bevat om gedownload te worden
        operation=FTPOperation.GET,
        create_intermediate_dirs=True,
    )

    @task()
    def read_csv_and_process():
        # Lees gedownloade file, pas eenvoudige Pandas data cleaning toe
        # Schrijf content van het DataFrame weg naar tabel in DWH
        # Schrijf content van het DataFrame weg naar een mongodb collection
        FILE_AIRFLOW_BASED = "data/raw_data.csv"
        # FILE_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/airflow/data/raw_data.csv"
        df_csv = pd.read_csv(FILE_AIRFLOW_BASED, dtype={"id": str, "naam": str, "salaris": np.int32})    # Laad downloaded file op Host
        df_csv.loc[df_csv['salaris'] < 1500, 'salaris'] = 1500      # Data cleaning: ken een minimum salaris toe
        write_csv_content_to_dwh(df_csv)
        write_csv_content_to_mongodb(df_csv)

    def write_csv_content_to_dwh(df_csv):
        engine = create_engine("postgresql+psycopg://dwh:dwh@dwh:5432/dwh")
        df_csv.to_sql("test", con=engine, if_exists="append", index=False)
    
    def write_csv_content_to_mongodb(df_csv):
        CONNECTION_STRING = "mongodb://test:test@mongodb:27017"
        with MongoClient(CONNECTION_STRING) as client:
            db = client["test"]
            collection_test = db["test_documents"]
            json_str = df_csv.to_json()
            json_object = json.loads(json_str)
            collection_test.insert_one(json_object)
    
    @task()
    def write_test_data_to_bigquery():
        # LOCAL RUN: os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/jkeustermans/JOpleiding/Data-Engineering/Project/airflow/keys/google_jkeustermans_key.json'
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True, # Automatically detects schema if the table is new
        )    

        rows_to_insert = [
            { u"id": 1, u"naam": "Jimmy", u"gemeente": "Mortsel" },
            { u"id": 2, u"naam": "Jan", u"gemeente": "Kontich" },
        ]

        table_id = Table.from_string("upbeat-isotope-142823.global_pps.Test")

        load_job = client.load_table_from_json(
            rows_to_insert, 
            table_id, 
            job_config=job_config
        )

        load_job.result()  # Wait for the job to finish

        print(f"Successfully loaded {load_job.output_rows} rows.")

        client.close()

    # read_csv_and_process.__wrapped__()

    download_task >> read_csv_and_process() >> upload_task >> write_test_data_to_bigquery()

dag = Test_Ftp()