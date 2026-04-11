from airflow.providers.ftp.operators.ftp import FTPFileTransmitOperator, FTPOperation
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from olap_offload_processor import OLAPOffloadProcessor
from decimal import *
from bigquery_offload_processor import BigQueryOffloadProcessor
from documentdb_offload_processor import DocumentDBOffloadProcessor

FILE_TREATMENTS = "data/medical_data/Outpatient_Treatments.csv"
FILE_PATIENTS = "data/medical_data/Outpatient_Registrations.csv"
FILE_SUBREGIONS = "data/medical_data/Subregions.csv"
FILE_COUNTRIES = "data/medical_data/Countries.csv"
FILE_SURVEYS = "data/medical_data/Surveys.csv"
FILE_INSTITUTIONS = "data/medical_data/Institutions.csv"
FILE_UNIT_REGISTRATIONS = "data/medical_data/Unit_Registrations.csv"

DAG_RUN_DB_HOST = "dwh"
DAG_RUN_DB_PORT = 5432
DAG_RUN_DB_NAME = "dwh"
DAG_RUN_DB_USER = "dwh"
DAG_RUN_DB_PASSWORD = "dwh"

DAG_RUN_MONGODB_HOST = "mongodb"
DAG_RUN_MONGODB_PORT = 27017
DAG_RUN_MONGODB_DBNAME = "pps"
DAG_RUN_MONGODB_USER = "test"
DAG_RUN_MONGODB_PASSWORD = "test"

CLOUD_RECORD_LIMIT = 5

dag_offload_default_args = {
    "retries": 1,
    "retry_delay": 5,
}

@dag(
    dag_id="offload_medical_data",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
    default_args=dag_offload_default_args,
)
def Offload_OLAP():
    # Download Files vanaf de FTP Server
    download_task = FTPFileTransmitOperator(
        task_id="download_medical_data_from_ftp",
        ftp_conn_id="ftp_server",
        local_filepath=[                            # path in container met bind volume waar raw data naar gedownload zal worden
            "data/medical_data/Countries.csv",
            "data/medical_data/Diagnosis.csv",
            "data/medical_data/Indications.csv",
            "data/medical_data/Institutions.csv",
            "data/medical_data/Outpatient_Registrations.csv",
            "data/medical_data/Outpatient_Treatments.csv",
            "data/medical_data/Subregions.csv",
            "data/medical_data/Surveys.csv",
            "data/medical_data/Unit_Registrations.csv"
        ],
        remote_filepath=[                           # path op ftp server met file die raw data bevat om gedownload te worden
            "outbound/medical_input_data/Countries.csv",
            "outbound/medical_input_data/Diagnosis.csv",
            "outbound/medical_input_data/Indications.csv",
            "outbound/medical_input_data/Institutions.csv",
            "outbound/medical_input_data/Outpatient_Registrations.csv",
            "outbound/medical_input_data/Outpatient_Treatments.csv",
            "outbound/medical_input_data/Subregions.csv",
            "outbound/medical_input_data/Surveys.csv",
            "outbound/medical_input_data/Unit_Registrations.csv"
        ],
        operation=FTPOperation.GET,
        create_intermediate_dirs=True,
    )

    @task()
    def offload_to_dwh_database():
        olap_offload_processor = OLAPOffloadProcessor(
            FILE_TREATMENTS,
            FILE_PATIENTS,
            FILE_SUBREGIONS,
            FILE_COUNTRIES,
            FILE_SURVEYS,
            FILE_INSTITUTIONS,
            FILE_UNIT_REGISTRATIONS,
            DAG_RUN_DB_HOST,
            DAG_RUN_DB_PORT,
            DAG_RUN_DB_NAME,
            DAG_RUN_DB_USER,
            DAG_RUN_DB_PASSWORD
        )
        olap_offload_processor.start_offload()

    @task()
    def offload_to_mongodb():
        documentdb_offload_processor = DocumentDBOffloadProcessor(
            FILE_TREATMENTS,
            FILE_PATIENTS,
            FILE_SUBREGIONS,
            FILE_COUNTRIES,
            FILE_SURVEYS,
            FILE_INSTITUTIONS,
            FILE_UNIT_REGISTRATIONS,
            DAG_RUN_MONGODB_HOST,
            DAG_RUN_MONGODB_PORT,
            DAG_RUN_MONGODB_USER,
            DAG_RUN_MONGODB_PASSWORD
        )
        documentdb_offload_processor.offload_medical_data_to_documentdb()

    @task()
    def offload_to_bigquery():
        # LOCAL RUN: 
        # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/jkeustermans/JOpleiding/Data-Engineering/Project/airflow/data/keys/google_jkeustermans_key.json'
        
        bigquery_offload_processor = BigQueryOffloadProcessor(
            DAG_RUN_DB_HOST, 
            DAG_RUN_DB_PORT, 
            DAG_RUN_DB_NAME, 
            DAG_RUN_DB_USER, 
            DAG_RUN_DB_PASSWORD,
            CLOUD_RECORD_LIMIT)
        bigquery_offload_processor.start_offload()

    # local run
    # offload_to_bigquery()

    download_task >> offload_to_dwh_database() >> offload_to_mongodb() >> offload_to_bigquery()

olap_offload_dag = Offload_OLAP()