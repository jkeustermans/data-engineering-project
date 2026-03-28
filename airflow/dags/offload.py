import pandas as pd
import numpy as np
import sqlalchemy as db

FILE_TREATMENTS_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/data_landingzone/Outpatient_Treatments.csv"
FILE_PATIENTS_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/data_landingzone/Outpatient_Registrations.csv"
FILE_SUBREGIONS_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/data_landingzone/Subregions.csv"
FILE_COUNTRIES_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/data_landingzone/Countries.csv"
FILE_SURVEYS_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/data_landingzone/Surveys.csv"
FILE_INSTITUTIONS_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/data_landingzone/Institutions.csv"
FILE_UNIT_REGISTRATIONS_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/data_landingzone/Unit_Registrations.csv"

class OLAPOffloadProcessor:

    def __init__(self, input_file_patients, input_file_treatments):
        self.input_file_patients = input_file_patients
        self.input_file_treatments = input_file_treatments
    
    def start_offload(self):
        self.__offload_patients()
        self.__offload_geographic()
        self.__offload_survey()
        self.__offload_department()
        self.__offload_treatments()

    def __convert_value_to_boolean(self, val):
        if (pd.isnull(val)): return None
        elif (val == '1' or val == 'Y' or val.upper() == 'YES'): return True
        else: return False

    def __read_treatments(self):
        return pd.read_csv(FILE_TREATMENTS_LOCAL_RUN, sep="|", dtype={
            'id': 'str',
            'outpatient_id': 'str',
            'atc5_code': 'str',
            'prescription_type': 'str',
            'single_unit_dose': np.float32,
            'dose_unit': 'str',
            'daily_doses': np.float32,
            'therapy_intended_duration_known': 'str',
            'therapy_intended_duration': np.float32,
            'diagnosis_code': 'str',
            'indication_code': 'str',
            'reason_in_notes': 'str',
            'reference_guideline_exists': 'str',
            'drug_according_to_guideline': 'str',
            'dose_according_to_guideline': 'str',
            'duration_according_to_guideline': 'str',
            'roa_according_to_guideline': 'str'
        })
    
    def __read_patients(self):
        return pd.read_csv(FILE_PATIENTS_LOCAL_RUN, sep="|", dtype={
            'id': 'str',
            'unit_registration_id': 'str',
            'age_group': 'str',
            'gender': 'str',
            'weight': np.float32,
            'birth_weight': np.float32,
            'symptom_codes': 'str'
        })
    
    def __read_subregions(self):
        return pd.read_csv(FILE_SUBREGIONS_LOCAL_RUN, sep="|", dtype={
            'sub_region_code': 'str',
            'sub_region_name': 'str'
        })

    def __read_countries(self):
        return pd.read_csv(FILE_COUNTRIES_LOCAL_RUN, sep="|", dtype={
            'iso': 'str',
            'name': 'str',
            'sub_region_code': 'str',
        })

    def __read_surveys(self):
        return pd.read_csv(FILE_SURVEYS_LOCAL_RUN, sep="|", dtype={
            'id': np.int32,
            'id_inquiry': np.int32,
            'id_institution': np.int32,
        })

    def __read_institutions(self):
        return pd.read_csv(FILE_INSTITUTIONS_LOCAL_RUN, sep="|", dtype={
            'institution_id': np.int32,
            'institution_name': 'str',
            'country_code': 'str',
            'sub_region_code': 'str',
            'institution_subtype_code': 'str',
        })

    def __read_unit_registrations(self):
        return pd.read_csv(FILE_UNIT_REGISTRATIONS_LOCAL_RUN, sep="|", dtype={
            'id': 'str',
            'survey_id': np.int32,
            'medical_specialty_type': 'str',
            'nbr_of_doctors': 'Int32',
            'nbr_of_pharmacists': 'Int32'
    }, parse_dates=['survey_date'])

    # Code die lokaal tegen een database uitgevoerd wordt (niet in een Apache Airflow context)
    def __offload_treatments(self):
        # Voorbereiding Treatments DataFrame
        df_treatments = self.__read_treatments()
        df_treatments["therapy_intended_duration_known"] = df_treatments["therapy_intended_duration_known"].apply(lambda val: self.__convert_value_to_boolean(val)).astype(bool)
        df_treatments = df_treatments.rename(columns = { "id": "treatment_id" })
        # df_departments = 
        
        # Voorbereiding Outpatients DataFrame
        df_outpatients = self.__read_patients()
        df_outpatients = df_outpatients.rename(columns = { "id": "outpatient_id" })

        # Join van DataSets
        # Opgelet Foreign keys zijn nog niet opgenomen
        df_merged = df_treatments.merge(df_outpatients[['outpatient_id', 'unit_registration_id', 'weight', 'birth_weight']], how="left", left_on = "outpatient_id", right_on = "outpatient_id")
        # df_merged = df_merged.merge( met department -> survey_id invvullen
        df_merged = df_merged.rename(columns = { "weight": "patient_weight" })
        df_merged = df_merged.rename(columns = { "birth_weight": "patient_birth_weight" })
        df_merged = df_merged.rename(columns = { "unit_registration_id": "department_id" })
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
            'department_id']]

        print(df_merged.info())
        with db.create_engine("postgresql+psycopg://dwh:dwh@localhost:5433/dwh").connect() as conn:
            conn.begin()                                                   # Start Transactie
            truncate_query = db.text("TRUNCATE TABLE fact_treatments")
            conn.execute(truncate_query)
            df_merged.to_sql("fact_treatments", con=conn, if_exists="append", index=False)
            conn.commit()                                                  # Commit Transactie

    def __offload_patients(self):
        # Voorbereiding Outpatients DataFrame
        df_outpatients = self.__read_patients()
        df_outpatients = df_outpatients.rename(columns = { "id": "patient_id" })
        df_outpatients = df_outpatients.drop(columns=["unit_registration_id", "weight", "birth_weight"])

        with db.create_engine("postgresql+psycopg://dwh:dwh@localhost:5433/dwh").connect() as conn:
            conn.begin()                                                   # Start Transactie
            truncate_query = db.text("TRUNCATE TABLE dim_patient")
            conn.execute(truncate_query)
            df_outpatients.to_sql("dim_patient", con=conn, if_exists="append", index=False)
            conn.commit()                                                  # Commit Transactie
    
    def __offload_geographic(self):
        # Voorbereiding Geographic DataFrame
        df_subregions = self.__read_subregions()
        df_countries = self.__read_countries()
        df_merged = df_countries.merge(df_subregions, how="left", left_on = "sub_region_code", right_on = "sub_region_code")
        df_merged = df_merged.rename(columns = {
            'iso': 'country_iso',
            'name': 'country_name'
        })
        df_merged = df_merged.reset_index().rename(columns={'index': 'geographic_id'})
        with db.create_engine("postgresql+psycopg://dwh:dwh@localhost:5433/dwh").connect() as conn:
            conn.begin()                                                   # Start Transactie
            truncate_query = db.text("TRUNCATE TABLE dim_geographic")
            conn.execute(truncate_query)
            df_merged.to_sql("dim_geographic", con=conn, if_exists="append", index=False)
            conn.commit()                                                  # Commit Transactie
    
    def __convert_inquiry_id_to_year_and_sequence(self, id):
        if id == 30: return (2024, 1)
        elif id ==  31: return (2024, 2)
        elif id ==  32: return (2024, 3)
        elif id ==  33: return (2025, 1)
        elif id ==  34: return (2025, 2)
        elif id ==  35: return (2025, 3)
        elif id ==  36: return (2026, 1)
        else: return (2026, 2)

    def __offload_survey(self):
        # Voorbereiding Survey DataFrame
        df_surveys = self.__read_surveys()
        sr_inquiry_year_sequence = df_surveys["id_inquiry"].apply(lambda d: self.__convert_inquiry_id_to_year_and_sequence(d))
        df_surveys["year"] = sr_inquiry_year_sequence.map(lambda d: d[0])
        df_surveys["period_seq_number"] = sr_inquiry_year_sequence.map(lambda d: d[1])
        df_surveys = df_surveys.rename(columns={'id': 'survey_id'})
        df_surveys = df_surveys.drop(columns=["id_inquiry", "id_institution"])
        with db.create_engine("postgresql+psycopg://dwh:dwh@localhost:5433/dwh").connect() as conn:
            conn.begin()                                                   # Start Transactie
            truncate_query = db.text("TRUNCATE TABLE dim_survey")
            conn.execute(truncate_query)
            df_surveys.to_sql("dim_survey", con=conn, if_exists="append", index=False)
            conn.commit()                                                  # Commit Transactie

    def __offload_department(self):
        # Voorbereiding Department DataFrame
        df_surveys = self.__read_surveys()
        df_institutions = self.__read_institutions()
        df_unit_registrations = self.__read_unit_registrations()
        df_unit_registrations = df_unit_registrations.rename(columns = { "id": "unit_registration_id" })
        df_merged = df_unit_registrations.merge(df_surveys, how="left", left_on = "survey_id", right_on = "id")
        df_merged = df_merged.rename(columns = { "id": "survey_id" })
        df_merged = df_merged.merge(df_institutions, how="left", left_on="id_institution", right_on="institution_id")
        df_merged = df_merged[["unit_registration_id", "medical_specialty_type", "nbr_of_doctors", "nbr_of_pharmacists", "institution_id", "institution_name", "institution_subtype_code"]]
        df_merged = df_merged.rename(columns = { "unit_registration_id": "department_id" })
        df_merged = df_merged.rename(columns = { "medical_specialty_type": "medical_specialty_type_code" })
        
        print(df_merged.info())
        with db.create_engine("postgresql+psycopg://dwh:dwh@localhost:5433/dwh").connect() as conn:
            conn.begin()                                                   # Start Transactie
            truncate_query = db.text("TRUNCATE TABLE dim_department")
            conn.execute(truncate_query)
            df_merged.to_sql("dim_department", con=conn, if_exists="append", index=False)
            conn.commit()                                                  # Commit Transactie

# Main code
offload: OLAPOffloadProcessor = OLAPOffloadProcessor(FILE_PATIENTS_LOCAL_RUN, FILE_TREATMENTS_LOCAL_RUN)
offload.start_offload()