# covid-nlp


Files:

	* data_schema.py
		- Patient, Visit, Event classes and JSON encoder/decoder
	* generate_patient_db.py
		- Read in meddra extractions from batch files and build patient knowlege graph
		- Example usage: python src/generate_patient_db.py --output_dir /home/colbyham/covid-nlp/output --use_dask
	* mental_health_analysis.py
		- Load patient knowledge graph from file and perform mental health queries
		- Example usage: python src/mental_health_analysis.py --patient_db_path /home/colbyham/covid-nlp/output/patients_20200831-050502.jsonl 
	* patient_db.py
		- PatientDB class

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

TODO:

	1. Read in patient demographic information
	2. Add aggregation by patient demographics attributes 
