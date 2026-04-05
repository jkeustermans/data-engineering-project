import pandas as pd
import numpy as np
import sqlalchemy as db
import psycopg as psy
from sqlalchemy import text

class DatawarehouseDAO:

    def __init__(self, host, port, dbname, user, password):
        self.db_host = host
        self.db_port = port
        self.db_name = dbname
        self.db_user = user
        self.db_password = password

    def determine_dwh_database_url(self):
        return f"postgresql+psycopg://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    def read_diagnosis_from_database(self):
        with db.create_engine(self.determine_dwh_database_url()).connect() as conn:
            return pd.read_sql("SELECT * FROM dim_diagnosis", con=conn, dtype={
                'diagnosis_id': np.int32,
                'code': 'str',
                'label': 'str'
            })

    def read_indications_from_database(self):
        with db.create_engine(self.determine_dwh_database_url()).connect() as conn:
            return pd.read_sql("SELECT * FROM dim_indication", con=conn, dtype={
                'indication_id': np.int32,
                'code': 'str',
                'label': 'str'
            })

    def read_dim_geographic_from_database(self):
        with db.create_engine(self.determine_dwh_database_url()).connect() as conn:
            return pd.read_sql("SELECT geographic_id, country_iso FROM dim_geographic", con=conn, dtype={
                'geographic_id': np.int32,
                'country_iso': 'str'
            })
    
    def persist_dim_department(self, dim_department_dataframe: pd.DataFrame):
        with db.create_engine(self.determine_dwh_database_url()).connect() as conn:
            conn.begin()   # Start Transactie
            truncate_query = db.text("TRUNCATE TABLE dim_department")
            conn.execute(truncate_query)
            dim_department_dataframe.to_sql("dim_department", con=conn, if_exists="append", index=False)
            conn.commit()  # Commit Transactie
    
    def persist_dim_survey(self, dim_survey_datafrrame: pd.DataFrame):
        with db.create_engine(self.determine_dwh_database_url()).connect() as conn:
            conn.begin()    # Start Transactie
            truncate_query = db.text("TRUNCATE TABLE dim_survey")
            conn.execute(truncate_query)
            dim_survey_datafrrame.to_sql("dim_survey", con=conn, if_exists="append", index=False)
            conn.commit()   # Commit Transactie
    
    def persist_dim_geographic(self, dim_geographic_dataframe: pd.DataFrame):
        with db.create_engine(self.determine_dwh_database_url()).connect() as conn:
            conn.begin()                                                   # Start Transactie
            truncate_query = db.text("TRUNCATE TABLE dim_geographic")
            conn.execute(truncate_query)
            dim_geographic_dataframe.to_sql("dim_geographic", con=conn, if_exists="append", index=False)
            conn.commit()                                                  # Commit Transactie
    
    def persist_dim_patient(self, dim_patients_dataframe: pd.DataFrame):
        with db.create_engine(self.determine_dwh_database_url()).connect() as conn:
            conn.begin()                                                   # Start Transactie
            truncate_query = db.text("TRUNCATE TABLE dim_patient")
            conn.execute(truncate_query)
            dim_patients_dataframe.to_sql("dim_patient", con=conn, if_exists="append", index=False)
            conn.commit()                                                  # Commit Transactie

    def persist_dim_indication(self):
        # Kon ook rechtstreeks met pandas gedaan worden maar voor educatieve doeleinden is er hier even een andere manier van werken gebruikt
        with db.create_engine(self.determine_dwh_database_url()).connect() as conn:
            conn.begin()    # Start Transactie
            metadata = db.MetaData()    # Extractie van metadata
            table_indications = db.Table('dim_indication', metadata, autoload_with=conn)    # Table object
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

    def persist_dim_diagnosis(self):
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
            
    def persist_facts_treatment(self, facts_treatment_dataframe: pd.DataFrame):
        with db.create_engine(self.determine_dwh_database_url()).connect() as conn:
            conn.begin()                                                   # Start Transactie
            truncate_query = db.text("TRUNCATE TABLE fact_treatments")
            conn.execute(truncate_query)
            facts_treatment_dataframe.to_sql("fact_treatments", con=conn, if_exists="append", index=False)
            conn.commit()                                                  # Commit Transactie
