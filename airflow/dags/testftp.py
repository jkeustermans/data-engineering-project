from airflow import DAG
from airflow.providers.ftp.operators.ftp import FTPFileTransmitOperator, FTPOperation
from datetime import datetime, timedelta
from airflow.sdk import dag, task

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
        print('ditiseentest')
        with open("data/processed_data.csv", "r") as file:
            content = file.read()
            print(content)

    download_task >> read_csv_and_process() >> upload_task

dag = Test_Ftp()