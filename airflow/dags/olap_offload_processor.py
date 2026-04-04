import pandas as pd
import numpy as np
import sqlalchemy as db
import psycopg as psy
from sqlalchemy import text

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
        self.input_csv_treatments = input_csv_treatments
        self.input_csv_patients = input_csv_patients
        self.input_csv_subregions = input_csv_subregions
        self.input_csv_countries = input_csv_countries
        self.input_csv_surveys = input_csv_surveys
        self.input_csv_institutions = input_csv_institutions
        self.input_csv_unit_registrations = input_csv_unit_registrations
        self.db_host = host
        self.db_port = port
        self.db_name = dbname
        self.db_user = user
        self.db_password = password
    
    def start_offload(self):
        self.__offload_dim_patients()
        self.__offload_dim_geographic()
        self.__offload_dim_survey()
        self.__offload_dim_department()
        self.__offload_dim_diagnosis()
        self.__offload_dim_indications()
        self.__offload_fact_treatments()

    def __convert_value_to_boolean(self, val):
        if (pd.isnull(val)): return None
        elif (val == '1' or val == 'Y' or val.upper() == 'YES'): return True
        else: return False

    def __read_treatments_from_input_csv(self):
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
    
    def __read_patients_from_input_csv(self):
        return pd.read_csv(self.input_csv_patients, sep="|", dtype={
            'id': 'str',
            'unit_registration_id': 'str',
            'age_group': 'str',
            'gender': 'str',
            'weight': np.float32,
            'birth_weight': np.float32,
            'symptom_codes': 'str'
        })
    
    def __read_subregions_from_input_csv(self):
        return pd.read_csv(self.input_csv_subregions, sep="|", dtype={
            'sub_region_code': 'str',
            'sub_region_name': 'str'
        })

    def __read_countries_from_input_csv(self):
        return pd.read_csv(self.input_csv_countries, sep="|", dtype={
            'iso': 'str',
            'name': 'str',
            'sub_region_code': 'str',
        })

    def __read_surveys_from_input_csv(self):
        return pd.read_csv(self.input_csv_surveys, sep="|", dtype={
            'id': np.int32,
            'id_inquiry': np.int32,
            'id_institution': np.int32,
        })

    def __read_institutions_from_input_csv(self):
        return pd.read_csv(self.input_csv_institutions, sep="|", dtype={
            'institution_id': np.int32,
            'institution_name': 'str',
            'country_code': 'str',
            'sub_region_code': 'str',
            'institution_subtype_code': 'str',
        })

    def __read_unit_registrations_from_input_csv(self):
        return pd.read_csv(self.input_csv_unit_registrations, sep="|", dtype={
            'id': 'str',
            'survey_id': np.int32,
            'medical_specialty_type': 'str',
            'nbr_of_doctors': 'Int32',
            'nbr_of_pharmacists': 'Int32'
    }, parse_dates=['survey_date'])

    def read_diagnosis_from_database(self):
        with db.create_engine(self.__determine_dwh_database_url()).connect() as conn:
            return pd.read_sql("SELECT * FROM dim_diagnosis", con=conn, dtype={
                'diagnosis_id': np.int32,
                'code': 'str',
                'label': 'str'
            })

    def read_indications_from_database(self):
        with db.create_engine(self.__determine_dwh_database_url()).connect() as conn:
            return pd.read_sql("SELECT * FROM dim_indication", con=conn, dtype={
                'indication_id': np.int32,
                'code': 'str',
                'label': 'str'
            })

    def read_dim_geographic_from_database(self):
        with db.create_engine(self.__determine_dwh_database_url()).connect() as conn:
            return pd.read_sql("SELECT geographic_id, country_iso FROM dim_geographic", con=conn, dtype={
                'geographic_id': np.int32,
                'country_iso': 'str'
            })

    # Offload code van alle dimension en facts tabellen
    def __offload_fact_treatments(self):
        # Voorbereiding Treatments DataFrame
        df_treatments = self.__read_treatments_from_input_csv()
        df_treatments["therapy_intended_duration_known"] = df_treatments["therapy_intended_duration_known"].apply(lambda val: self.__convert_value_to_boolean(val)).astype(bool)
        df_treatments = df_treatments.rename(columns = { "id": "treatment_id" })
        
        # Outpatients (Link + Degenerate Dimension)
        df_outpatients = self.__read_patients_from_input_csv()
        df_outpatients = df_outpatients.rename(columns = { "id": "outpatient_id" })
        df_merged = df_treatments.merge(df_outpatients[['outpatient_id', 'unit_registration_id', 'weight', 'birth_weight']], how="left", left_on = "outpatient_id", right_on = "outpatient_id")
        df_merged = df_merged.rename(columns = { "weight": "patient_weight" })
        df_merged = df_merged.rename(columns = { "birth_weight": "patient_birth_weight" })
        df_merged = df_merged.rename(columns = { "unit_registration_id": "department_id" })

        # Link Geographic & Department & Survey
        df_linked_data_set = self.__retrieve_linked_dataset()
        df_merged = df_merged.merge(df_linked_data_set, how="left", left_on="outpatient_id", right_on="patient_id")

        # Link Diagnosis
        df_diagnosis = self.read_diagnosis_from_database()
        df_merged = df_merged.merge(df_diagnosis, how="left", left_on="diagnosis_code", right_on="code")
        df_merged = df_merged.drop(columns=["diagnosis_code", "diagnosis_id", "label"])
        df_merged = df_merged.rename(columns = { "code": "diagnosis_id" })
        
        # Link Indications
        df_indications = self.read_indications_from_database()
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

        with db.create_engine(self.__determine_dwh_database_url()).connect() as conn:
            conn.begin()                                                   # Start Transactie
            truncate_query = db.text("TRUNCATE TABLE fact_treatments")
            conn.execute(truncate_query)
            df_merged.to_sql("fact_treatments", con=conn, if_exists="append", index=False)
            conn.commit()                                                  # Commit Transactie

    def __retrieve_linked_dataset(self):
        # Link een aantal datasets aan mekaar voor bepalen foreign keys in fact_treatments table 
        df_patients = self.__read_patients_from_input_csv()
        df_surveys = self.__read_surveys_from_input_csv()
        df_institutions = self.__read_institutions_from_input_csv()
        df_unit_registrations = self.__read_unit_registrations_from_input_csv()
        df_patients = df_patients.rename(columns={'id': 'patient_id'})
        df_merged = df_patients.merge(df_unit_registrations, how="left", left_on="unit_registration_id", right_on="id")
        df_merged = df_merged.merge(df_surveys, how="left", left_on="survey_id", right_on="id")
        df_merged = df_merged.merge(df_institutions, how="left", left_on="id_institution", right_on="institution_id")
        df_dim_geographic = self.read_dim_geographic_from_database()
        df_merged = df_merged.merge(df_dim_geographic, how="left", left_on="country_code", right_on="country_iso")
        return df_merged[["patient_id", "survey_id", "geographic_id"]]

    def __offload_dim_patients(self):
        # Voorbereiding Outpatients DataFrame
        df_outpatients = self.__read_patients_from_input_csv()
        df_outpatients = df_outpatients.rename(columns = { "id": "patient_id" })
        df_outpatients = df_outpatients.drop(columns=["unit_registration_id", "weight", "birth_weight"])

        with db.create_engine(self.__determine_dwh_database_url()).connect() as conn:
            conn.begin()                                                   # Start Transactie
            truncate_query = db.text("TRUNCATE TABLE dim_patient")
            conn.execute(truncate_query)
            df_outpatients.to_sql("dim_patient", con=conn, if_exists="append", index=False)
            conn.commit()                                                  # Commit Transactie
    
    def __offload_dim_geographic(self):
        # Voorbereiding Geographic DataFrame
        df_subregions = self.__read_subregions_from_input_csv()
        df_countries = self.__read_countries_from_input_csv()
        df_merged = df_countries.merge(df_subregions, how="left", left_on = "sub_region_code", right_on = "sub_region_code")
        df_merged = df_merged.rename(columns = {
            'iso': 'country_iso',
            'name': 'country_name'
        })
        df_merged = df_merged.reset_index().rename(columns={'index': 'geographic_id'})
        with db.create_engine(self.__determine_dwh_database_url()).connect() as conn:
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

    def __offload_dim_survey(self):
        # Voorbereiding Survey DataFrame
        df_surveys = self.__read_surveys_from_input_csv()
        sr_inquiry_year_sequence = df_surveys["id_inquiry"].apply(lambda d: self.__convert_inquiry_id_to_year_and_sequence(d))
        df_surveys["year"] = sr_inquiry_year_sequence.map(lambda d: d[0])
        df_surveys["period_seq_number"] = sr_inquiry_year_sequence.map(lambda d: d[1])
        df_surveys = df_surveys.rename(columns={'id': 'survey_id'})
        df_surveys = df_surveys.drop(columns=["id_inquiry", "id_institution"])
        with db.create_engine(self.__determine_dwh_database_url()).connect() as conn:
            conn.begin()    # Start Transactie
            truncate_query = db.text("TRUNCATE TABLE dim_survey")
            conn.execute(truncate_query)
            df_surveys.to_sql("dim_survey", con=conn, if_exists="append", index=False)
            conn.commit()   # Commit Transactie
    
    def __offload_dim_diagnosis(self):
        # Kon ook rechtstreeks met pandas gedaan worden maar voor educatieve doeleinden is er hier even een andere manier van werken gebruikt
        with psy.connect(host=self.db_host, port=self.db_port, dbname=self.db_name, user=self.db_user, password=self.db_password) as conn:
            with conn.transaction():
                conn.execute("TRUNCATE TABLE dim_diagnosis")
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (1, 'Proph CNS', 'Prophylaxis for CNS (meningococcal)'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (2, 'CNS', 'Infections of the Central Nervous System'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (3, 'Proph CVS', 'Cardiac or Vascular prophylaxis, endocarditis prophylaxis'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (4, 'CVS', 'CardioVascular System infections: endocarditis, endovascular device e.g pacemaker, vascular graft'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (5, 'Proph DEN', 'Prophylaxis for dental cases'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (6, 'DEN', 'Dental infections e.g. abscess, pulpitis, periodontal disease'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (7, 'Proph ENT', 'Prophylaxis for Ear, Nose, Throat including mouth (Surgical or Medical prophylaxis)'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (8, 'AOM', 'Acute otitis media and CSOM (Chronic Suppurative Otitis Media)'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (9, 'Proph EYE', 'Prophylaxis for Eye operations'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (10, 'EYE', 'Therapy for Eye infections e.g.,  Conjunctivitis, trachoma, blepharitis, keratitis'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (11, 'Proph GI', 'Gastro-Intestinal prophylaxis'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (12, 'GI', 'Any other gastro-intestinal infection'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (13, 'CDIF', 'Clostridioides difficile infection'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (14, 'Proph OBGY', 'Prophylaxis for OBstetric or GYnaecological surgery (MP: carriage of group B streptococcus)'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (15, 'OBGY', 'Obstetric/Gynaecological infections, Sexually Transmitted Diseases (STD) in women, vaginitis, bacteria vaginosis'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (16, 'GUM', 'Genito-Urinary Males + Prostatitis, epididymo-orchitis, STD in men'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (17, 'Malaria', 'Malaria'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (18, 'HIV', 'Human immunodeficiency virus'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (19, 'PUO', 'Pyrexia of Unknown Origin - Fever syndrome with no identified source or site of infection'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (20, 'LO-LYMPH', 'Localized acute lymphadenitis'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (21, 'LYMPH', 'Lymphatics as the primary source of infection. Suppurative lymphadenitis'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (22, 'Other', 'Antimicrobial prescribed with documentation but no defined diagnosis group'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (23, 'MP-GEN', 'Drug is used as Medical Prophylaxis in general, without targeting a specific site, e.g. antifungal prophylaxis during immunosuppression'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (24, 'UNK', 'Completely Unknown Indication'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (25, 'PROK', 'Antimicrobial (e.g. erythromycin) prescribed for Prokinetic use'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (26, 'Proph RESP', 'Prophylaxis for Respiratory pathogens e.g. for aspergillosis'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (27, 'LUNG', 'Lung abscess including aspergilloma'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (28, 'URTI', 'Upper Respiratory Tract viral Infections including influenza but not ENT'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (29, 'Bron', 'Acute Bronchitis or exacerbations of chronic bronchitis'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (30, 'Bronch', 'Acute bronchiolitis'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (31, 'Pneu', 'Pneumonia or LRTI (lower respiratory tract infections)'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (32, 'COVID-19', 'Coronavirus disease caused by SARS-CoV-2 infection'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (33, 'TB', 'Pulmonary TB - Tuberculosis / Extrapulmonary TB'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (34, 'CF', 'Complication of cystic fibrosis'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (35, 'Proph SST', 'Prophylaxis for Skin and Soft Tissue, impetigo, plastic or orthopaedic surgery'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (36, 'SST', 'Skin and Soft Tissue: Cellulitis, impetigo, erysipelas, folliculitis, other viral exanthems, burn wound- and bite-related infections'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (37, 'Sys-DI', 'Disseminated infection (viral infections such as measles, Cytomegalovirus...)'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (38, 'DST', 'Deep Soft Tissue not involving bone e.g., infected pressure or diabetic ulcer, abscess'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (39, 'Proph UTI', 'Prophylaxis for recurrent Urinary Tract Infection (Medical Prophylaxis)'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (40, 'Cys', 'Lower Urinary Tract Infection (UTI), cystitis'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (41, 'Pye', 'Upper UTI including catheter related urinary tract infection, pyelonephritis'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (42, 'ASB', 'Asymptomatic bacteriuria'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (43, 'ENT', 'Therapy for Ear, Nose, Throat infections, other than PHAR, SIN or AOM'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (44, 'BAC', 'Bacteraemia or fungaemia with no clear anatomic site and no shock'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (45, 'SEPSIS', 'Sepsis of any origin'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (46, 'PHAR', 'Therapy for pharyngitis'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (47, 'SIN', 'Therapy for sinusitis'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (48, 'GO', 'Acute infectious diarrhoea/gastroenteritis'))
                conn.execute("INSERT INTO dim_diagnosis (diagnosis_id, code, label) values(%s, %s, %s)", (49, 'Typh-fever', 'Typhoid fever/enteric fever'))

    def __offload_dim_indications(self):
        # Kon ook rechtstreeks met pandas gedaan worden maar voor educatieve doeleinden is er hier even een andere manier van werken gebruikt
        with db.create_engine(self.__determine_dwh_database_url()).connect() as conn:
            conn.begin()    # Start Transactie
            metadata = db.MetaData()    # Extractie van metadata
            table_indications = db.Table('dim_indication', metadata, autoload_with=conn, )    # Table object
            conn.execute(text("TRUNCATE TABLE dim_indication"))
            conn.execute(db.insert(table_indications).values(indication_id=1, code="CAI", label="Community Acquired Infection"))
            conn.execute(db.insert(table_indications).values(indication_id=2, code="HAI1", label="Post-operative surgical site infection"))
            conn.execute(db.insert(table_indications).values(indication_id=3, code="HAI2", label="Intervention related or any other Healthcare Associated Infection"))
            conn.execute(db.insert(table_indications).values(indication_id=4, code="HAI3", label="Clostridium Difficile Associated Diarrhoea (CDAD)"))
            conn.execute(db.insert(table_indications).values(indication_id=5, code="MP", label="Medical prophylaxis"))
            conn.execute(db.insert(table_indications).values(indication_id=6, code="OTH", label="Other"))
            conn.execute(db.insert(table_indications).values(indication_id=7, code="SP1", label="Surgical prophylaxis - single dose"))
            conn.execute(db.insert(table_indications).values(indication_id=8, code="SP2", label="Surgical prophylaxis - one day"))
            conn.execute(db.insert(table_indications).values(indication_id=9, code="SP3", label="Surgical prophylaxis - >1 day"))
            conn.execute(db.insert(table_indications).values(indication_id=10, code="UNK", label="Completely unknown indication"))
            conn.commit()   # Commit Transactie

    def __offload_dim_department(self):
        # Voorbereiding Department DataFrame
        df_surveys = self.__read_surveys_from_input_csv()
        df_institutions = self.__read_institutions_from_input_csv()
        df_unit_registrations = self.__read_unit_registrations_from_input_csv()
        df_unit_registrations = df_unit_registrations.rename(columns = { "id": "unit_registration_id" })
        df_merged = df_unit_registrations.merge(df_surveys, how="left", left_on = "survey_id", right_on = "id")
        df_merged = df_merged.rename(columns = { "id": "survey_id" })
        df_merged = df_merged.merge(df_institutions, how="left", left_on="id_institution", right_on="institution_id")
        df_merged = df_merged[["unit_registration_id", "medical_specialty_type", "nbr_of_doctors", "nbr_of_pharmacists", "institution_id", "institution_name", "institution_subtype_code"]]
        df_merged = df_merged.rename(columns = { "unit_registration_id": "department_id" })
        df_merged = df_merged.rename(columns = { "medical_specialty_type": "medical_specialty_type_code" })
        
        with db.create_engine(self.__determine_dwh_database_url()).connect() as conn:
            conn.begin()   # Start Transactie
            truncate_query = db.text("TRUNCATE TABLE dim_department")
            conn.execute(truncate_query)
            df_merged.to_sql("dim_department", con=conn, if_exists="append", index=False)
            conn.commit()  # Commit Transactie
    
    def __determine_dwh_database_url(self):
        return f"postgresql+psycopg://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

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