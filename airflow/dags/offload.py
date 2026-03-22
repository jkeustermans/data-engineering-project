import pandas as pd
import numpy as np
import sqlalchemy as db

def convert_value_to_boolean(val):
    if (pd.isnull(val)): return None
    elif (val == '1' or val == 'Y' or val.upper() == 'YES'): return True
    else: return False

# Code die lokaal tegen een database uitgevoerd wordt (niet in een Apache Airflow context)
def offload_treatments():
    FILE_TREATMENTS_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/data_landingzone/Outpatient_Treatments.csv"
    FILE_PATIENTS_LOCAL_RUN = "/home/jkeustermans/JOpleiding/Data-Engineering/Project/data_landingzone/Outpatient_Registrations.csv"
    
    # Voorbereiding Treatments DataFrame
    df_treatments = pd.read_csv(FILE_TREATMENTS_LOCAL_RUN, sep="|", dtype={
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
    df_treatments["therapy_intended_duration_known"] = df_treatments["therapy_intended_duration_known"].apply(lambda val: convert_value_to_boolean(val)).astype(bool)
    df_treatments = df_treatments.rename(columns = { "id": "treatment_id" })
    
    # Voorbereiding Outpatients DataFrame
    df_outpatients = pd.read_csv(FILE_PATIENTS_LOCAL_RUN, sep="|", dtype={
        'id': 'str',
        'unit_registration_id': 'str',
        'age_group': 'str',
        'gender': 'str',
        'weight': np.float32,
        'birth_weight': np.float32,
        'symptom_codes': 'str'
    })
    df_outpatients = df_outpatients.rename(columns = { "id": "outpatient_id" })

    # Join van DataSets
    # Opgelet Foreign keys zijn nog niet opgenomen
    df_merged = df_treatments.merge(df_outpatients[['outpatient_id', 'weight', 'birth_weight']], how="left", left_on = "outpatient_id", right_on = "outpatient_id")
    df_merged = df_merged.rename(columns = { "weight": "patient_weight" })
    df_merged = df_merged.rename(columns = { "birth_weight": "patient_birth_weight" })
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
        'outpatient_id']]

    print(df_merged.info())
    with db.create_engine("postgresql+psycopg://dwh:dwh@localhost:5433/dwh").connect() as conn:
        conn.begin()                                                   # Start Transactie
        truncate_query = db.text("TRUNCATE TABLE fact_treatments")
        conn.execute(truncate_query)
        df_merged.to_sql("fact_treatments", con=conn, if_exists="append", index=False)
        conn.commit()                                                  # Commit Transactie

# Main code
offload_treatments()