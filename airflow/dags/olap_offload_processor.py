import pandas as pd
import numpy as np
import sqlalchemy as db
import psycopg as psy
from sqlalchemy import text
from olap_csv_reader import OLAPCSVReader
from dwh_dao import DatawarehouseDAO

FILE_TREATMENTS_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/data_landingzone/Outpatient_Treatments.csv"
FILE_PATIENTS_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/data_landingzone/Outpatient_Registrations.csv"
FILE_SUBREGIONS_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/data_landingzone/Subregions.csv"
FILE_COUNTRIES_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/data_landingzone/Countries.csv"
FILE_SURVEYS_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/data_landingzone/Surveys.csv"
FILE_INSTITUTIONS_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/data_landingzone/Institutions.csv"
FILE_UNIT_REGISTRATIONS_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/data_landingzone/Unit_Registrations.csv"

LOCAL_RUN_DB_HOST = "localhost"
LOCAL_RUN_DB_PORT = 5433
LOCAL_RUN_DB_NAME = "dwh"
LOCAL_RUN_DB_USER = "dwh"
LOCAL_RUN_DB_PASSWORD = "dwh"

class OLAPOffloadProcessor:

    def __init__(self, input_csv_treatments, input_csv_patients, input_csv_subregions, input_csv_countries, 
                 input_csv_surveys, input_csv_institutions, input_csv_unit_registrations,
                 host, port, dbname, user, password):
        self.olap_csv_reader = OLAPCSVReader(
            input_csv_treatments,
            input_csv_patients,
            input_csv_subregions,
            input_csv_countries,
            input_csv_surveys,
            input_csv_institutions,
            input_csv_unit_registrations
        )
        self.dwhDAO = DatawarehouseDAO(host, port, dbname, user, password)
    
    def start_offload(self):
        self.__offload_dim_patients()
        self.__offload_dim_geographic()
        self.__offload_dim_survey()
        self.__offload_dim_department()
        self.__offload_dim_diagnosis()
        self.__offload_dim_indication()
        self.__offload_fact_treatments()

    # Offload code van alle dimension en facts tabellen
    def __offload_fact_treatments(self):
        # Voorbereiding Treatments DataFrame
        df_treatments = self.olap_csv_reader.read_treatments_from_input_csv()
        df_treatments["therapy_intended_duration_known"] = df_treatments["therapy_intended_duration_known"].apply(lambda val: self.__convert_value_to_boolean(val)).astype(bool)
        df_treatments = df_treatments.rename(columns = { "id": "treatment_id" })
        
        # Outpatients (Link + Degenerate Dimension)
        df_outpatients = self.olap_csv_reader.read_patients_from_input_csv()
        df_outpatients = df_outpatients.rename(columns = { "id": "outpatient_id" })
        df_merged = df_treatments.merge(df_outpatients[['outpatient_id', 'unit_registration_id', 'weight', 'birth_weight']], how="left", left_on = "outpatient_id", right_on = "outpatient_id")
        df_merged = df_merged.rename(columns = { "weight": "patient_weight" })
        df_merged = df_merged.rename(columns = { "birth_weight": "patient_birth_weight" })
        df_merged = df_merged.rename(columns = { "unit_registration_id": "department_id" })

        # Link Geographic & Department & Survey
        df_linked_data_set = self.__retrieve_linked_dataset()
        df_merged = df_merged.merge(df_linked_data_set, how="left", left_on="outpatient_id", right_on="patient_id")

        # Link Diagnosis
        df_diagnosis = self.dwhDAO.read_diagnosis_from_database()
        df_merged = df_merged.merge(df_diagnosis, how="left", left_on="diagnosis_code", right_on="code")
        df_merged = df_merged.drop(columns=["diagnosis_code", "diagnosis_id", "label"])
        df_merged = df_merged.rename(columns = { "code": "diagnosis_id" })
        
        # Link Indications
        df_indications = self.dwhDAO.read_indications_from_database()
        df_merged = df_merged.merge(df_indications, how="left", left_on="indication_code", right_on="code")
        df_merged = df_merged.drop(columns=["indication_code", "indication_id", "label"])
        df_merged = df_merged.rename(columns = { "code": "indication_id" })
        
        df_merged = df_merged[[
            'treatment_id',
            'patient_weight',
            'patient_birth_weight',
            'atc5_code',
            'prescription_type',
            'single_unit_dose',
            'dose_unit',
            'daily_doses',
            'therapy_intended_duration_known',
            'therapy_intended_duration',
            'reason_in_notes',
            'reference_guideline_exists',
            'drug_according_to_guideline',
            'dose_according_to_guideline',
            'duration_according_to_guideline',
            'roa_according_to_guideline',
            'outpatient_id',
            'department_id',
            'diagnosis_id',
            'indication_id',
            'geographic_id',
            'survey_id']]        
        self.dwhDAO.persist_facts_treatment(df_merged)

    def __retrieve_linked_dataset(self):
        # Link een aantal datasets aan mekaar voor bepalen foreign keys in fact_treatments table 
        df_patients = self.olap_csv_reader.read_patients_from_input_csv()
        df_surveys = self.olap_csv_reader.read_surveys_from_input_csv()
        df_institutions = self.olap_csv_reader.read_institutions_from_input_csv()
        df_unit_registrations = self.olap_csv_reader.read_unit_registrations_from_input_csv()
        df_patients = df_patients.rename(columns={'id': 'patient_id'})
        df_merged = df_patients.merge(df_unit_registrations, how="left", left_on="unit_registration_id", right_on="id")
        df_merged = df_merged.merge(df_surveys, how="left", left_on="survey_id", right_on="id")
        df_merged = df_merged.merge(df_institutions, how="left", left_on="id_institution", right_on="institution_id")
        df_dim_geographic = self.dwhDAO.read_dim_geographic_from_database()
        df_merged = df_merged.merge(df_dim_geographic, how="left", left_on="country_code", right_on="country_iso")
        return df_merged[["patient_id", "survey_id", "geographic_id"]]

    def __offload_dim_patients(self):
        # Voorbereiding Outpatients DataFrame
        df_outpatients = self.olap_csv_reader.read_patients_from_input_csv()
        df_outpatients = df_outpatients.rename(columns = { "id": "patient_id" })
        df_outpatients = df_outpatients.drop(columns=["unit_registration_id", "weight", "birth_weight"])
        self.dwhDAO.persist_dim_patient(df_outpatients)
    
    def __offload_dim_geographic(self):
        # Voorbereiding Geographic DataFrame
        df_subregions = self.olap_csv_reader.read_subregions_from_input_csv()
        df_countries = self.olap_csv_reader.read_countries_from_input_csv()
        df_merged = df_countries.merge(df_subregions, how="left", left_on = "sub_region_code", right_on = "sub_region_code")
        df_merged = df_merged.rename(columns = {
            'iso': 'country_iso',
            'name': 'country_name'
        })
        df_merged = df_merged.reset_index().rename(columns={'index': 'geographic_id'})
        self.dwhDAO.persist_dim_geographic(df_merged)
    
    def __offload_dim_survey(self):
        # Voorbereiding Survey DataFrame
        df_surveys = self.olap_csv_reader.read_surveys_from_input_csv()
        sr_inquiry_year_sequence = df_surveys["id_inquiry"].apply(lambda d: self.__convert_inquiry_id_to_year_and_sequence(d))
        df_surveys["year"] = sr_inquiry_year_sequence.map(lambda d: d[0])
        df_surveys["period_seq_number"] = sr_inquiry_year_sequence.map(lambda d: d[1])
        df_surveys = df_surveys.rename(columns={'id': 'survey_id'})
        df_surveys = df_surveys.drop(columns=["id_inquiry", "id_institution"])
        self.dwhDAO.persist_dim_survey(df_surveys)
    
    def __offload_dim_diagnosis(self):
        self.dwhDAO.persist_dim_diagnosis()

    def __offload_dim_indication(self):
        self.dwhDAO.persist_dim_indication()

    def __offload_dim_department(self):
        # Voorbereiding Department DataFrame
        df_surveys = self.olap_csv_reader.read_surveys_from_input_csv()
        df_institutions = self.olap_csv_reader.read_institutions_from_input_csv()
        df_unit_registrations = self.olap_csv_reader.read_unit_registrations_from_input_csv()
        df_unit_registrations = df_unit_registrations.rename(columns = { "id": "unit_registration_id" })
        df_merged = df_unit_registrations.merge(df_surveys, how="left", left_on = "survey_id", right_on = "id")
        df_merged = df_merged.rename(columns = { "id": "survey_id" })
        df_merged = df_merged.merge(df_institutions, how="left", left_on="id_institution", right_on="institution_id")
        df_merged = df_merged[["unit_registration_id", "medical_specialty_type", "nbr_of_doctors", "nbr_of_pharmacists", "institution_id", "institution_name", "institution_subtype_code"]]
        df_merged = df_merged.rename(columns = { "unit_registration_id": "department_id" })
        df_merged = df_merged.rename(columns = { "medical_specialty_type": "medical_specialty_type_code" })
        self.dwhDAO.persist_dim_department(df_merged)

    def __convert_inquiry_id_to_year_and_sequence(self, id):
        if id == 30: return (2024, 1)
        elif id ==  31: return (2024, 2)
        elif id ==  32: return (2024, 3)
        elif id ==  33: return (2025, 1)
        elif id ==  34: return (2025, 2)
        elif id ==  35: return (2025, 3)
        elif id ==  36: return (2026, 1)
        else: return (2026, 2)

    def __convert_value_to_boolean(self, val):
        if (pd.isnull(val)): return None
        elif (val == '1' or val == 'Y' or val.upper() == 'YES'): return True
        else: return False

# Main code
# offload: OLAPOffloadProcessor = OLAPOffloadProcessor(
#     FILE_TREATMENTS_LOCAL_RUN,
#     FILE_PATIENTS_LOCAL_RUN,
#     FILE_SUBREGIONS_LOCAL_RUN,
#     FILE_COUNTRIES_LOCAL_RUN,
#     FILE_SURVEYS_LOCAL_RUN,
#     FILE_INSTITUTIONS_LOCAL_RUN,
#     FILE_UNIT_REGISTRATIONS_LOCAL_RUN,
#     LOCAL_RUN_DB_HOST,
#     LOCAL_RUN_DB_PORT,
#     LOCAL_RUN_DB_NAME,
#     LOCAL_RUN_DB_USER,
#     LOCAL_RUN_DB_PASSWORD)
# offload.start_offload()