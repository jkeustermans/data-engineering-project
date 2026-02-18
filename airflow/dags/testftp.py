from airflow import DAG
from airflow.providers.ftp.operators.ftp import FTPFileTransmitOperator, FTPOperation
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from pathlib import Path
import pandas as pd
import numpy as np
import psycopg as pgs
from sqlalchemy import create_engine

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
        # Lees gedownloade file, pas eenvoudige Pandas data cleaning toe en schrijf content van het DataFrame weg naar tabel in DWH
        df_csv = pd.read_csv("data/raw_data.csv", dtype={"id": str, "naam": str, "salaris": np.int32})    # Laad downloaded file op Host
        df_csv.loc[df_csv['salaris'] < 1500, 'salaris'] = 1500      # Data cleaning: ken een minimum salaris toe
        engine = create_engine("postgresql+psycopg://dwh:dwh@dwh:5432/dwh")
        df_csv.to_sql("test", con=engine, if_exists="append", index=False)

    # read_csv_and_process.__wrapped__()

    download_task >> read_csv_and_process() >> upload_task

dag = Test_Ftp()