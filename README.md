# covid-nlp
Example usage: python src/generate_patient_db.py

Current functionality:
1. Get depression, anxiety, insomnia and distress diagnosis events from medDRA extractions
2. Build patient KG and assign events to the corrrect owners

Files:
	* data_schema.py
		* Patient, Visit, Event classes and JSON encoder/decoder
	* generate_patient_db.py
		* Read in meddra extractions from batch files and build patient knowlege graph
		* Example usage: python src/generate_patient_db.py --output_dir /home/colbyham/covid-nlp/output --use_dask
	* mental_health_analysis.py
		* Load patient knowledge graph from file and perform mental health queries
		* Example usage: python src/mental_health_analysis.py --patient_db_path /home/colbyham/covid-nlp/output/patients_20200831-050502.jsonl 
	* patient_db.py
		* PatientDB class

TODO:
	
