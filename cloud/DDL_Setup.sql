CREATE SCHEMA IF NOT EXISTS `upbeat-isotope-142823.pps`;

drop table `upbeat-isotope-142823.pps.dim_patient`;
drop table `upbeat-isotope-142823.pps.dim_geographic`;
drop table `upbeat-isotope-142823.pps.dim_survey`;
drop table `upbeat-isotope-142823.pps.dim_department`;
drop table `upbeat-isotope-142823.pps.dim_diagnosis`;
drop table `upbeat-isotope-142823.pps.dim_indication`;
drop table `upbeat-isotope-142823.pps.fact_treatments`;


create table pps.dim_patient(
	patient_id string(36),
	age_group string(20),
	gender string(20),
	symptom_codes string(500)
);

create table pps.dim_geographic(
	geographic_id Integer,
	country_iso string(36),
	country_name string(50),
	sub_region_code string(10),
	sub_region_name string(50)
);

create table pps.dim_survey(
	survey_id Integer,
	year Integer,
	period_seq_number Integer
);

create table pps.dim_department(
	department_id string(36),
	medical_specialty_type_code string(50),
	nbr_of_doctors Integer,
	nbr_of_pharmacists Integer,
	institution_id Integer,
	institution_name string(50),
	institution_subtype_code string(30)
);

create table pps.dim_diagnosis(
	diagnosis_id Integer,
	code string(50),
	label string(200)
);

create table pps.dim_indication(
	indication_id Integer,
	code string(50),
	label string(200)
);

create table pps.fact_treatments (
	treatment_id string(36),
	patient_weight decimal(5,2),
	patient_birth_weight decimal(5,2),
	atc5_code string(20),
	prescription_type string(20),
	single_unit_dose decimal(10,2),
	dose_unit string(20),
	daily_doses Integer,
	therapy_intended_duration_known bool,
	therapy_intended_duration decimal(5,2),
	reason_in_notes string(20),
	reference_guideline_exists string(20),
	drug_according_to_guideline string(20),
	dose_according_to_guideline string(20),
	duration_according_to_guideline string(20),
	roa_according_to_guideline string(20),
	outpatient_id string(40),
	diagnosis_id string(40),
	indication_id string(40),
	geographic_id string(40),
	survey_id string(40),
	department_id string(40)
);