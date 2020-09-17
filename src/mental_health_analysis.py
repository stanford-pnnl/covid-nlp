import argparse
import json
from collections import Counter

from patient_db import PatientDB, get_top_k


def match_terms(patients, terms):
    print(f"{patients}")
    patients_matched = PatientDB(name='patients_matched')
    matches = set()
    print(f"Matching terms: {terms}")
    for term in terms:
        term_patients_matched, term_matches = \
            patients.match_patients(
                f'{term}_patients_matched',
                term,
                event_keys=['diagnosis_name'],
                event_types=['DiagnosisEvent'])
        patients_matched.merge_patients(term_patients_matched)
        matches = matches.union(term_matches)
        print(f"{term_patients_matched}")
    print(f"{patients_matched}")
    return patients_matched, matches


def mental_health_co_morbidities(patients, search_terms):
    print("Running mental health co-morbidities checks...")
    patients_matched, matches = match_terms(patients, search_terms)

    event_counters, event_roles, event_entity_levels = \
        patients_matched.get_event_counters(
            event_types=['DiagnosisEvent'], meddra_roles=True)
    import pdb;pdb.set_trace()

    # Aggregate counts for each such diagnosis code either based on
    # the number of visits or number of patients
    print("Report top-k diagnosis codes")
    k, top_k = get_top_k(event_counters,
                         event_entity_levels,
                         event_roles,
                         k=10)
    for entity_level in top_k:
        print(f"Top {k} diagnosis roles per {entity_level}:")
        for key in sorted(top_k[entity_level]):
            values = top_k[entity_level][key]
            if values:
                print(f"\tkey: {key}\n\t\t{values}")
        print()
    import pdb;pdb.set_trace()

    print('Test split patient DB by monthly frequency key')
    monthly_splits = patients_matched.agg_time(time_freq='M')
    for month_db in monthly_splits.values():
        print(f"{month_db}")

    import pdb;pdb.set_trace()
    print("END OF PROGRAM")


def main(args):
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
    parser.add_argument('--patient_db_path',
                        help='PatientDB dump path', required=True)
    args: argparse.Namespace = parser.parse_args()
    main(args)
