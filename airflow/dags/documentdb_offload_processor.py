from constants import *
import pandas as pd
from csv_reader import CSVReader
from mongodb_dao import MongoDBDAO

class DocumentDBOffloadProcessor:

    def __init__(self, input_csv_treatments, input_csv_patients, input_csv_subregions, input_csv_countries, 
                 input_csv_surveys, input_csv_institutions, input_csv_unit_registration,
                 host, port, user, password):
        self.csv_reader = CSVReader(
            input_csv_treatments,
            input_csv_patients,
            input_csv_subregions,
            input_csv_countries,
            input_csv_surveys,
            input_csv_institutions,
            input_csv_unit_registration
        )
        self.mongoDao = MongoDBDAO(host, port, user, password)

    def offload_medical_data_to_documentdb(self):
        dataframe = self.__retrieve_fully_flat_linked_dataset()
        self.__write_csv_content_to_mongodb(dataframe)

    def __retrieve_fully_flat_linked_dataset(self):
        df_treatments = self.csv_reader.read_treatments_from_input_csv()
        df_patients = self.csv_reader.read_patients_from_input_csv()
        df_surveys = self.csv_reader.read_surveys_from_input_csv()
        df_institutions = self.csv_reader.read_institutions_from_input_csv()
        df_unit_registrations = self.csv_reader.read_unit_registrations_from_input_csv()
        df_countries = self.csv_reader.read_countries_from_input_csv()
        df_subregions = self.csv_reader.read_subregions_from_input_csv()

        df_treatments = df_treatments.rename(columns={'id': 'treatment_id'})
        df_patients = df_patients.rename(columns={
            'id': 'patient_id', 
            'age_group': 'patient_age_group',
            'gender': 'patient_gender',
            'weight': 'patient_weight',
            'birth_weight': 'patient_birth_weight',
            'symptom_codes': 'patient_symptom_codes'
        })
        df_countries = df_countries.rename(columns={'sub_region_code': 'subregion_code', 'name': 'country_name'})
        df_subregions = df_subregions.rename(columns={'sub_region_code': 'code_sub_region'})
        df_unit_registrations = df_unit_registrations.rename(columns={
            'medical_specialty_type': 'unit_medical_specialty_type',
            'nbr_of_doctors': 'unit_nbr_of_doctors',
            'nbr_of_pharmacists': 'unit_nbr_of_pharmacists'
        }) 

        df_merged = df_treatments.merge(df_patients, how="left", left_on="outpatient_id", right_on="patient_id")
        df_merged = df_merged.merge(df_unit_registrations, how="left", left_on="unit_registration_id", right_on="id")
        df_merged = df_merged.merge(df_surveys, how="left", left_on="survey_id", right_on="id")
        df_merged = df_merged.merge(df_institutions, how="left", left_on="id_institution", right_on="institution_id")
        df_merged = df_merged.merge(df_countries, how="left", left_on="country_code", right_on="iso")
        df_merged = df_merged.merge(df_subregions, how="left", left_on="sub_region_code", right_on="code_sub_region")

        df_merged = df_merged.rename(columns={
            'country_code': 'institution_country_code', 
            'country_name': 'institution_country_name',
            'sub_region_code': 'institution_sub_region_code',
            'sub_region_name': 'institution_sub_region_name'
        })
        
        return df_merged[[
            'treatment_id',
            'atc5_code',
            'prescription_type',
            'single_unit_dose',
            'dose_unit',
            'daily_doses',
            'therapy_intended_duration_known',
            'therapy_intended_duration',
            'diagnosis_code',
            'indication_code',
            'reason_in_notes',
            'reference_guideline_exists',
            'drug_according_to_guideline',
            'dose_according_to_guideline',
            'duration_according_to_guideline',
            'roa_according_to_guideline',
            'patient_age_group',
            'patient_gender',
            'patient_weight',
            'patient_birth_weight',
            'patient_symptom_codes',
            'survey_date',
            'unit_medical_specialty_type',
            'unit_nbr_of_doctors',
            'unit_nbr_of_pharmacists',
            'institution_name',
            'institution_subtype_code',
            'institution_country_code',
            'institution_country_name',
            'institution_sub_region_code',
            'institution_sub_region_name'
        ]]

    def __write_csv_content_to_mongodb(self, dataframe: pd.DataFrame):
        self.mongoDao.persist_dataframe_to_medical_documents_collection(dataframe)


LOCAL_RUN_MONGODB_HOST = "localhost"
LOCAL_RUN_MONGODB_PORT = 27017
LOCAL_RUN_MONGODB_USER = "test"
LOCAL_RUN_MONGODB_PASSWORD = "test"

local_processor = DocumentDBOffloadProcessor(
    FILE_TREATMENTS_LOCAL_RUN,
    FILE_PATIENTS_LOCAL_RUN,
    FILE_SUBREGIONS_LOCAL_RUN,
    FILE_COUNTRIES_LOCAL_RUN,
    FILE_SURVEYS_LOCAL_RUN,
    FILE_INSTITUTIONS_LOCAL_RUN,
    FILE_UNIT_REGISTRATIONS_LOCAL_RUN,
    LOCAL_RUN_MONGODB_HOST,
    LOCAL_RUN_MONGODB_PORT,
    LOCAL_RUN_MONGODB_USER,
    LOCAL_RUN_MONGODB_PASSWORD
)

local_processor.offload_medical_data_to_documentdb()