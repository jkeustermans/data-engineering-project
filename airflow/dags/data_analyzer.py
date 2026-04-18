import psycopg as psy
import pandas as pd
from dwh_dao import DatawarehouseDAO
import seaborn as sns
from constants import *
import matplotlib.pyplot as plt
from datetime import datetime
import shutil
import os

class DataAnalyzer:
    FILENAME_ANALYSIS_ATC3 = 'ATC3_Frequency.png'
    FILENAME_ANALYSIS_THERAPY_INTENDED_DURATION = 'Therapy_Intended_Duration.png'
    FILENAME_ANALYSIS_PRESCRIPTION_TYPE = 'Prescription_Types_Frequency.png'
    FILENAME_ANALYSIS_PATIENTS_WEIGHTS = 'Patients_Weight.png'
    FILENAME_ANALYSIS_PATIENTS_GENDER = 'Patients_Gender.png'
    FILENAME_ANALYSIS_TREATMENTS_GENERAL_ANALYSIS = 'Treatments_General_Analysis.csv'

    def __init__(self, output_location, host, port, dbname, user, password):
        self.output_location = output_location
        self.db_host = host
        self.db_port = port
        self.db_name = dbname
        self.db_user = user
        self.db_password = password
        self.dwhDAO = DatawarehouseDAO(host, port, dbname, user, password)

    def _read_dataset_treatments(self) -> pd.DataFrame:
        return self.dwhDAO.read_facts_treatment_from_database()

    def _read_dataset_patients(self) -> pd.DataFrame:
        return self.dwhDAO.read_dim_patient_from_database()

    def _analyze_atc5_categories(self, original_dataframe: pd.DataFrame):
        dataframe = original_dataframe.copy()
        dataframe['main_category_atc5'] = dataframe['atc5_code'].str.slice(stop=3)
        counts = dataframe['main_category_atc5'].value_counts()
        plot = sns.barplot(x=counts.index, y=counts.values, palette="dark", alpha=.6)
        plt.xlabel('ATC3 Category')
        plt.ylabel('Frequency')
        plt.title('Frequency ATC3 Antimicrobials in Treatments')
        plt.grid(axis='y', alpha=0.3)
        plotted_figure = plot.get_figure()
        self._save_figure(plotted_figure, self.FILENAME_ANALYSIS_ATC3)
        plt.close()

    def _analyze_prescription_types(self, original_dataframe: pd.DataFrame):
        dataframe = original_dataframe.copy()
        counts = dataframe['prescription_type'].value_counts()
        plot = sns.barplot(x=counts.index, y=counts.values, palette="dark", alpha=.6)
        plt.xlabel('Prescription Type')
        plt.ylabel('Frequency')
        plt.title('Frequency Prescription Types in Treatments')
        plt.grid(axis='y', alpha=0.3)
        plotted_figure = plot.get_figure()
        self._save_figure(plotted_figure, self.FILENAME_ANALYSIS_PRESCRIPTION_TYPE)
        plt.close()

    def _analyze_intended_duration_known(self, original_dataframe: pd.DataFrame):
        dataframe = original_dataframe.copy()
        dataframe['main_category_atc5'] = dataframe['atc5_code'].str.slice(stop=3)
        plt.figure(figsize=(15, 5))
        plot = sns.boxplot(data=dataframe, x=dataframe['therapy_intended_duration'], y=dataframe['main_category_atc5'])
        plt.title('Therapy Intended Duration')
        plt.xlabel('Duration')
        plt.ylabel('ATC3')
        plt.grid(axis='y', alpha=0.3)
        plotted_figure = plot.get_figure()
        self._save_figure(plotted_figure, self.FILENAME_ANALYSIS_THERAPY_INTENDED_DURATION)
        plt.close()

    def _analyze_patient_weights(self, original_dataframe: pd.DataFrame):
        dataframe = original_dataframe.copy()
        print(dataframe.info())
        plot = sns.histplot(data=dataframe, x="patient_weight", bins=10, binrange=(0, 150), palette="dark", alpha=.6, color='darkgreen')
        plt.xlabel('Weight (kg)')
        plt.ylabel('Frequency')
        plt.title('Patient Weights')
        plt.grid(axis='y', alpha=0.3)
        plotted_figure = plot.get_figure()
        self._save_figure(plotted_figure, self.FILENAME_ANALYSIS_PATIENTS_WEIGHTS)
        plt.close()
    
    def _analyze_genders(self, original_dataframe: pd.DataFrame):
        dataframe = original_dataframe.copy()
        counts = dataframe['gender'].value_counts()
        plot = sns.barplot(x=counts.index, y=counts.values, palette="dark", alpha=.6)
        plt.xlabel('Gender')
        plt.ylabel('Frequency')
        plt.title('Gender of Patients')
        plt.grid(axis='y', alpha=0.3)
        plotted_figure = plot.get_figure()
        self._save_figure(plotted_figure, self.FILENAME_ANALYSIS_PATIENTS_GENDER)
        plt.close()

    def _analyze_dataset_treatments_describe(self, original_dataframe: pd.DataFrame):
        dataframe = original_dataframe[['patient_weight', 'patient_birth_weight', 'single_unit_dose', 'daily_doses', 'therapy_intended_duration']]
        dataframe.describe().round(2).to_csv(self._determine_full_path_to_file(self.FILENAME_ANALYSIS_TREATMENTS_GENERAL_ANALYSIS))
    
    def _create_dir(self, dir):
        if os.path.exists(dir):
            shutil.rmtree(dir)
        os.mkdir(dir)
    
    def _save_figure(self, figure, name):
        figure.savefig(fname=self._determine_full_path_to_file(name))

    def _determine_full_path_to_file(self, name):
        return self.output_location + '/' + datetime.today().strftime('%Y%m%d') + '/' + name

    def start_analysis(self):
        dataframe_treatments = self._read_dataset_treatments()
        dataframe_patients = self._read_dataset_patients()
        self._create_dir(self.output_location + '/' + datetime.today().strftime('%Y%m%d'))
        self._analyze_atc5_categories(dataframe_treatments)
        self._analyze_prescription_types(dataframe_treatments)
        self._analyze_intended_duration_known(dataframe_treatments)
        self._analyze_patient_weights(dataframe_treatments)
        self._analyze_genders(dataframe_patients)
        self._analyze_dataset_treatments_describe(dataframe_treatments)
        
# LOCAL_RUN_DB_PORT = 5433
# data_analyzer = DataAnalyzer(DIR_ANALYSIS_OUTPUT_LOCAL_RUN, LOCAL_RUN_DB_HOST, LOCAL_RUN_DB_PORT, LOCAL_RUN_DB_NAME, LOCAL_RUN_DB_USER, LOCAL_RUN_DB_PASSWORD)
# data_analyzer.start_analysis()