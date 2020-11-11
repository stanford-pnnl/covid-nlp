# covid-nlp


Files/Modules:

	* data_schema.py
		- Patient, Visit, Event classes and JSON encoder/decoder
	* events.py
		- Event checking functions
	* generate.py
		- PatientDB generation functions
	* run_generate.py
		- Read in meddra extractions from batch files and build patient knowlege graph
		- Example usage: python src/generate_patient_db.py --output_dir /home/colbyham/covid-nlp/output --use_dask
	* run_mental_health_analysis.py
		- Load patient knowledge graph from file and perform mental health queries
		- Example usage: python src/run_mental_health_analysis.py --patient_db_path /home/colbyham/covid-nlp/output/patients_20200831-050502.jsonl
    * mental_health_analysis.py
        - Implementation of mental health queries
    * omop.py
		- Functions related to OMOP format tables
	* patient_db.py
		- PatientDB class
	* utils.py
		- Common functions that are shared between many modules.
	* ExampleNotebook.ipynb
		- Jupyter notebook version of run_mental_health_analysis.py

Data sources:

	* NLP extractions
    	* meddra_extractions
	* Structured EHR
    	* OMOP Common Data Model
        	* DRUG_EXPOSURE
        	* CONCEPT
      	* demographics

Features:

	* generate_patient_db.py
		1. Capture DiagnosisEvents
			* Depression
			* Anxiety
			* Insomnia
			* Distress
		2. Meddra matching levels
			* PT
			* concept_text
			* TODO: other meddra levels
	* patient_db.py
		1. Count matches per entity level (patient, visit, event)
		2. Patient aggregation by time ( Can match year, month, and day values)

Environment:

	- To create an identical env
		* conda create --name myenv --file spec-file.txt
	- To install listed packages into an existing environment
		* conda install --name myenv --file spec-file.txt

TODO:

	1. Allow for other levels of meddra matching
	2. Implement meddra queries
		* e.g. Match(SOC="*", HLGT="cardiac_valve_disorders", HLT="*", PT="*")
