from airflow.providers.ftp.operators.ftp import FTPFileTransmitOperator, FTPOperation
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from google.cloud import bigquery
from google.cloud.bigquery import Table
import os
import olap_offload_processor as oop
from decimal import *
import json
import pandas as pd

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

CLOUD_RECORD_LIMIT = 5;

olap_offload_processor = oop.OLAPOffloadProcessor(
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

dag_offload_olap_default_args = {
    "retries": 1,
    "retry_delay": 5,
}

@dag(
    dag_id="offload_olap",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
    default_args=dag_offload_olap_default_args,
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
        olap_offload_processor.start_offload()

    @task()
    def offload_to_bigquery():
        # LOCAL RUN: 
        # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/jkeustermans/JOpleiding/Data-Engineering/Project/airflow/data/keys/google_jkeustermans_key.json'

        processor = oop.OLAPOffloadProcessor(
                    FILE_TREATMENTS,
                    FILE_PATIENTS,
                    FILE_SUBREGIONS,
                    FILE_COUNTRIES,
                    FILE_SURVEYS,
                    FILE_INSTITUTIONS,
                    FILE_UNIT_REGISTRATIONS,
                    DAG_RUN_DB_HOST,    # "localhost",
                    DAG_RUN_DB_PORT,    # 5433,
                    DAG_RUN_DB_NAME,
                    DAG_RUN_DB_USER,
                    DAG_RUN_DB_PASSWORD
                )

        df_indications = processor.read_indications_from_database()
        df_indications = df_indications.head(CLOUD_RECORD_LIMIT)

        print(df_indications)

        print('Start offload!!!')
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True, # Automatically detects schema if the table is new
        )

        json_str_indications = df_indications.to_json(orient="records")

        json_indications = json.loads(json_str_indications);

        table_id = Table.from_string("upbeat-isotope-142823.pps.dim_indication")

        load_job = client.load_table_from_json(
            json_indications,
            table_id, 
            job_config=job_config
        )

        load_job.result()  # Wait for the job to finish

        print(f"Successfully loaded {load_job.output_rows} rows.")

        client.close()

    # local run
    # offload_to_bigquery()

    download_task >> offload_to_dwh_database() >> offload_to_bigquery()

olap_offload_dag = Offload_OLAP()