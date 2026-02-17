from airflow import DAG
from airflow.providers.ftp.operators.ftp import FTPFileTransmitOperator, FTPOperation
from datetime import datetime, timedelta

default_args = {
    "retries": 2,
    "retry_delay": 30,
}

with DAG(
    dag_id="test_ftp_dag",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
    default_args=default_args,
) as dag:
    upload_task = FTPFileTransmitOperator(
        task_id="upload_to_ftp",
        ftp_conn_id="ftp_server",
        # local_filepath="/home/jkeustermans/JOpleiding/Data-Engineering/Project/airflow/data/data.csv",
        local_filepath="/opt/airflow/data/data.csv",
        remote_filepath="data.csv",
        operation=FTPOperation.PUT,
        create_intermediate_dirs=False,
    )
    download_task = FTPFileTransmitOperator(
        task_id="download_from_ftp",
        ftp_conn_id="ftp_server",
        local_filepath="/opt/airflow/data/data2.csv",
        remote_filepath="test.txt",
        operation=FTPOperation.GET,
        create_intermediate_dirs=False,
    )