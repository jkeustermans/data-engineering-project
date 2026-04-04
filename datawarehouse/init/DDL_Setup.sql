create table dim_patient(
	patient_id varchar(36),
	age_group varchar(20),
	gender varchar(20),
	symptom_codes varchar(500)
);

create table dim_geographic(
	geographic_id decimal(5),
	country_iso varchar(36),
	country_name varchar(50),
	sub_region_code varchar(10),
	sub_region_name varchar(50)
);

create table dim_survey(
	survey_id decimal(5),
	year decimal(4),
	period_seq_number decimal(2)
);

create table dim_department(
	department_id varchar(36),
	medical_specialty_type_code varchar(50),
	nbr_of_doctors decimal(2),
	nbr_of_pharmacists decimal(2),
	institution_id decimal(5),
	institution_name varchar(50),
	institution_subtype_code varchar(30)
);

create table dim_diagnosis(
	diagnosis_id decimal(5),
	code varchar(50),
	label varchar(200)
);

create table dim_indication(
	indication_id decimal(5),
	code varchar(50),
	label varchar(200)
);

create table fact_treatments (
	treatment_id varchar(36),
	patient_weight decimal(5,2),
	patient_birth_weight decimal(5,2),
	atc5_code varchar(20),
	prescription_type varchar(20),
	single_unit_dose decimal(10,2),
	dose_unit varchar(20),
	daily_doses decimal(5),
	therapy_intended_duration_known bool,
	therapy_intended_duration decimal(5,2),
	reason_in_notes varchar(20),
	reference_guideline_exists varchar(20),
	drug_according_to_guideline varchar(20),
	dose_according_to_guideline varchar(20),
	duration_according_to_guideline varchar(20),
	roa_according_to_guideline varchar(20),
	outpatient_id varchar(40),
	diagnosis_id varchar(40),
	indication_id varchar(40),
	geographic_id varchar(40),
	survey_id varchar(40),
	department_id varchar(40)
);