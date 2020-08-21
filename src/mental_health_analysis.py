from patient_db import PatientDB
#def load_patients_db(path):
#    #with open(path, 'r') as f:
#    #    # Use json encoder to load patient DB


def mental_health_co_morbidities(patients, terms):
    patient_key_matches = set()
    for term in terms:
        term_patient_key_matches = pat_db.get_patients(entity_class='Event', entity_attribute='diagnosis')
        patient_key_matches.union(term_patient_key_matches)

    # Lookup information from visits where these codes were reported and collect other diagnossis code

    # Aggregate counts for each such diagnosis code either based on the number of visits or number of patients


def main():
    patients_output_dir = "/home/colbyham/covid-nlp/output"
    patients_db_path = f"{patient_output_dir}/patients_20200821-031648.jsonl"

    # Create and load an instance of PatientDB
    patients = PatientDB(name='all')
    patients.load(patients_db_path)
    import pdb;pdb.set_trace()

    # Q1
    #question_one_terms = ['depression', 'anxiety', 'insomnia', 'distress']
    #answer_one = mental_health_co_morbidities(patients, question_one_terms)

    # Q2
    #question_two_terms = question_one_terms
    #mental_health_age_distribution(patients, question_two_terms)

    # Q3
    #question_three_terms = question_one_terms
    #mental_health_age_sex_distribution(patients, question_three_terms)

if __name__ == "__main__":
    main()
