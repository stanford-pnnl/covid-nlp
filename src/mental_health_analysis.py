from patient_db import PatientDB, get_top_k
import json
from collections import Counter
import argparse


def mental_health_co_morbidities(patients, search_terms):
    print("Running mental health co-morbidities checks...")
    patients_matched = PatientDB()
    matches = set()
    print(f"search_terms: {search_terms}")
    for term in search_terms:
        term_patients_matched, term_matches = \
                patients.match_patients(
                        term,
                        event_keys=['diagnosis_name'],
                        event_types=['DiagnosisEvent'])
        patients_matched.merge_patients(term_patients_matched)
        print(f"\tterm: {term}, len(term_matches): {len(term_matches)}")
    
        matches = matches.union(term_matches)
    print(f"{len(patients_matched.patients)} patients matched")

    # Lookup information from visits where these codes were reported and collect other diagnosis code
    patient_ids = set()
    visit_ids = set()
    event_ids = set()
    keys = set()
    terms = set()
    for match in matches:
        patient_id, visit_id, event_id, key, term = match
        patient_ids.add(patient_id)
        visit_ids.add(visit_id)
        event_ids.add(event_id)
        keys.add(key)
        terms.add(term)

    diagnosis_event_counters, \
            diagnosis_event_roles, \
            diagnosis_event_entity_levels, \
            diagnosis_event_count_per_entity_level = \
            patients_matched.get_event_counters(event_types=['DiagnosisEvent'])
    for entity_level, entity_count in diagnosis_event_count_per_entity_level.items():
        print(f"entity_level: {entity_level}, entity_count: {entity_count}")

    # Aggregate counts for each such diagnosis code either based on the number of visits or number of patients
    patient_matched_stats = patients_matched.get_stats()
    import pdb;pdb.set_trace()
    print("Report top-k diagnosis codes")
    # Report top-k diagnosis codes
    k, top_k = get_top_k(diagnosis_event_counters, diagnosis_event_entity_levels, diagnosis_event_roles, k=10)
    for entity_level in top_k:
        print(f"Top {k} diagnosis codes per {entity_level}:")
        #{top_k[entity_level]}")
        for key, values in top_k[entity_level].items():
            print(f"\tkey: {key}\n\t\t{values}")
        print()
    import pdb;pdb.set_trace()

    print('Test split patient DB by monthly frequency key')
    monthly_splits = patients_matched.agg_time(time_freq='M')

    import pdb;pdb.set_trace()
    print("END OF PROGRAM")


def main(args):
    data_dir = "/home/colbyham/covid-nlp/output"
    patients_db_path = f"{data_dir}/patients_20200831-050502.jsonl"

    # Create and load an instance of PatientDB
    patients = PatientDB(name='all')
    patients.load(args.patient_db_path)

    print("Question 1:")
    question_one_terms = ['Depression', 'Anxiety', 'Insomnia', 'Distress']
    answer_one = mental_health_co_morbidities(patients, question_one_terms)

    # Q2
    #question_two_terms = question_one_terms
    #mental_health_age_distribution(patients, question_two_terms)

    # Q3
    #question_three_terms = question_one_terms
    #mental_health_age_sex_distribution(patients, question_three_terms)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--patient_db_path', help='PatientDB dump path', required=True)
    args: argparse.Namespace = parser.parse_args()
    main(args)
