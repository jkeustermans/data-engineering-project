from dwh_dao import DatawarehouseDAO
from google.cloud import bigquery
from google.cloud.bigquery import Table
import json
import pandas as pd

class BigQueryOffloadProcessor:
    GOOGLE_PROJECT_ID = "upbeat-isotope-142823"
    BIGQUERY_SCHEMA = "pps"

    def __init__(self, db_host, db_port, db_name, db_user, db_password, record_limit):
        self.dwhDAO = DatawarehouseDAO(db_host, db_port, db_name, db_user, db_password)
        self.record_limit = record_limit

    def start_offload(self):
        self.__offload_dim_indications_to_bigquery()
        self.__offload_dim_diagnosis_to_bigquery()
        self.__offload_dim_geographic_to_bigquery()
        self.__offload_dim_department_to_bigquery()
        self.__offload_dim_survey_to_bigquery()
        self.__offload_dim_patient_to_bigquery()
        self.__offload_facts_treatments_to_bigquery()
    
    def __offload_dim_indications_to_bigquery(self):
        df_indications = self.dwhDAO.read_indications_from_database()
        self.__persist_dataframe_to_biquery_table(df_indications, "dim_indication")

    def __offload_dim_diagnosis_to_bigquery(self):
        df_diagnosis = self.dwhDAO.read_diagnosis_from_database()
        self.__persist_dataframe_to_biquery_table(df_diagnosis, "dim_diagnosis")

    def __offload_dim_geographic_to_bigquery(self):
        df_geographic = self.dwhDAO.read_dim_geographic_from_database()
        self.__persist_dataframe_to_biquery_table(df_geographic, "dim_geographic")

    def __offload_dim_department_to_bigquery(self):
        df_department = self.dwhDAO.read_dim_department_from_database()
        df_department["institution_id"] = pd.to_numeric(df_department["institution_id"], errors="coerce").astype("Int64")
        df_department["nbr_of_doctors"] = pd.to_numeric(df_department["nbr_of_doctors"], errors="coerce").astype("Int64")
        df_department["nbr_of_pharmacists"] = pd.to_numeric(df_department["nbr_of_pharmacists"], errors="coerce").astype("Int64")
        self.__persist_dataframe_to_biquery_table(df_department, "dim_department")

    def __offload_dim_survey_to_bigquery(self):
        df_survey = self.dwhDAO.read_dim_survey_from_database()
        self.__persist_dataframe_to_biquery_table(df_survey, "dim_survey")

    def __offload_dim_patient_to_bigquery(self):
        df_patient = self.dwhDAO.read_dim_patient_from_database()
        self.__persist_dataframe_to_biquery_table(df_patient, "dim_patient")

    def __offload_facts_treatments_to_bigquery(self):
        df_treatment = self.dwhDAO.read_facts_treatment_from_database()
        print(df_treatment.info())
        df_treatment["daily_doses"] = pd.to_numeric(df_treatment["daily_doses"], errors="coerce").astype("Int32")
        df_treatment["geographic_id"] = pd.to_numeric(df_treatment["geographic_id"], errors="coerce").astype("Int32")
        df_treatment["survey_id"] = pd.to_numeric(df_treatment["survey_id"], errors="coerce").astype("Int32")
        self.__persist_dataframe_to_biquery_table(df_treatment, "fact_treatments")

    def __persist_dataframe_to_biquery_table(self, dataframe: pd.DataFrame, table_name: str):
        # LOCAL RUN: 
        # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/jkeustermans/JOpleiding/Data-Engineering/Project/airflow/data/keys/google_jkeustermans_key.json'
        
        reduced_dataframe = dataframe.copy().head(self.record_limit)
        
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        )

        json_string_dataframe = reduced_dataframe.to_json(orient="records")

        json_objects = json.loads(json_string_dataframe)

        table_id = Table.from_string(f"{self.GOOGLE_PROJECT_ID}.{self.BIGQUERY_SCHEMA}.{table_name}")

        load_job = client.load_table_from_json(
            json_objects,
            table_id, 
            job_config=job_config
        )

        load_job.result()  # Wacht tot alles opgeladen is

        client.close()