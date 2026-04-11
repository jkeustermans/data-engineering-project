import pandas as pd
import numpy as np

class CSVReader:
     def __init__(self, input_csv_treatments, input_csv_patients, input_csv_subregions, input_csv_countries, 
                 input_csv_surveys, input_csv_institutions, input_csv_unit_registrations):
        self.input_csv_treatments = input_csv_treatments
        self.input_csv_patients = input_csv_patients
        self.input_csv_subregions = input_csv_subregions
        self.input_csv_countries = input_csv_countries
        self.input_csv_surveys = input_csv_surveys
        self.input_csv_institutions = input_csv_institutions
        self.input_csv_unit_registrations = input_csv_unit_registrations
     
     def read_treatments_from_input_csv(self):
        return pd.read_csv(self.input_csv_treatments, sep="|", dtype={
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
     
     def read_patients_from_input_csv(self):
        return pd.read_csv(self.input_csv_patients, sep="|", dtype={
            'id': 'str',
            'unit_registration_id': 'str',
            'age_group': 'str',
            'gender': 'str',
            'weight': np.float32,
            'birth_weight': np.float32,
            'symptom_codes': 'str'
        })
    
     def read_subregions_from_input_csv(self):
        return pd.read_csv(self.input_csv_subregions, sep="|", dtype={
            'sub_region_code': 'str',
            'sub_region_name': 'str'
        })

     def read_countries_from_input_csv(self):
        return pd.read_csv(self.input_csv_countries, sep="|", dtype={
            'iso': 'str',
            'name': 'str',
            'sub_region_code': 'str',
        })

     def read_surveys_from_input_csv(self):
        return pd.read_csv(self.input_csv_surveys, sep="|", dtype={
            'id': np.int32,
            'id_inquiry': np.int32,
            'id_institution': np.int32,
        })

     def read_institutions_from_input_csv(self):
        return pd.read_csv(self.input_csv_institutions, sep="|", dtype={
            'institution_id': np.int32,
            'institution_name': 'str',
            'country_code': 'str',
            'sub_region_code': 'str',
            'institution_subtype_code': 'str',
        })

     def read_unit_registrations_from_input_csv(self):
        return pd.read_csv(self.input_csv_unit_registrations, sep="|", dtype={
            'id': 'str',
            'survey_id': np.int32,
            'medical_specialty_type': 'str',
            'nbr_of_doctors': 'Int32',
            'nbr_of_pharmacists': 'Int32'
    }, parse_dates=['survey_date'])